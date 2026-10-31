# One Fallback Rule, Revised Under Test

Between October 30 and 31, 2026, the rule that decides when to start the direct
YouTube backup changed repeatedly. Each revision fixed the problem in front
of it. Isolated testing or review of the next step then found a gap that the
fix had created or left open. This page tells that sequence as one case study.
The current rule is the [fallback decision contract](fallback-decision-contract.md).

## Starting Point

Since October 20, a Dell controller had started the backup after 30 s of
unhealthy *or unobservable* relay health, and stopped it after 300 s of
continuous health. A production drill on October 20 stopped the relay by hand
and the backup started after 52 s. See the
[route and drill](self-healing-oci-fanout.md#drill-stopping-the-relay-2026-10-20-0855-to-0903-jst).
Treating "cannot observe" like "failed" was simple and caught that outage.

## The Sequence

| Step | Trigger | Change | What the next check found |
| --- | --- | --- | --- |
| 1 | Oct 30: a relay timestamp 3 ms ahead of Dell was rejected as "from the future". The backup started without a media fault, and repeated rejections kept resetting the stop window. | Accept up to 1 s of future skew (provisional). | One second matched an earlier public-page fix, not a measured bound. |
| 2 | Review of the provisional tolerance. | Replace it with a nonce-bound clock interval: oldest possible age at most 20 s, not wholly more than 5 ms in the future. | A clock failure still counted as route failure. |
| 3 | A clock fault is not a media fault. | Causal policy: start only on confirmed MEDIA_FAILURE. Clock and observation loss hold the route. Add a durable cause ID and arena's secondary-recovery veto. | A real input-loss test showed that the relay facts producer discarded fresh copy error facts from an idle bridge. The producer was fixed. |
| 4 | Isolated relay stop with the operated FFmpeg retry settings. | None at first: no backup in 75 s. Short retries kept changing the FFmpeg generation, every new generation needed a fresh baseline, and the failure window never reached 30 s. | The hold in step 3 had made a real relay outage look like harmless observation loss. |
| 5 | That failed test. | The relay facts producer reports a stopped relay unit explicitly. A 90 s deadline survives FFmpeg generation changes. ACK progress renews the deadline. | Review: an ACK proves Dell-to-relay delivery only. A stopped or hung YouTube copy process left ACKs advancing, and a deterministic trace started nothing in 600 s. |
| 6 | That review. | Report copy STOPPED and STALLED explicitly. Renew the 90 s deadline only from complete-route health. Keep the clock hold only while ACKs advance, and alert after 300 s. | Review: after a long clock hold, one HTTP 503 hit an already expired deadline and started the backup immediately. |
| 7 | That trace. | A non-clock gap after a clock hold gets its own 90 s grace. Brief gaps keep the hold age. | Further audit found that alternating clock labels could renew grace without downstream proof, while missing ACK observation could start backup despite downstream flow. |
| 8 | That transition audit. | Verify positive media progress with bound boottime freshness; renew availability only from counter proof. Retain short-gap baselines while resetting healthy time. | Review found that UTC still prevented retiring a healthy backup, although media freshness no longer needed UTC. |
| 9 | Review of duplicated freshness mechanisms and timers. | Use bound boottime freshness for media health and backup retirement. Keep UTC as diagnostics and an independent arena veto. Remove transition grace; prefer verified downstream progress over a conflicting local export. | See the current decision contract and dated simplification audit for validation and limits. |

Step 4 is a failed test rather than a policy change.
Each change was applied to production without stopping the broadcast. Faults
were injected only into owned, isolated processes, never into the live route.
Each gap was therefore present in production until the next step replaced it;
none was exercised by a recorded production outage.

## What Went Wrong, In General Terms

**An overcorrection traded one error for a worse one.** Step 3 was right
that clock faults are not media faults. It then extended the same treatment to
all observation loss. For this system a missed start is unbounded viewer
outage, while a false start is a bounded second upload. Holding on uncertainty
inverted that priority. The fix was to bound uncertainty in time, not to
lengthen media retries to suit the detector: extending the FFmpeg retry wait
made the old test pass, and it was rejected for that reason.

**Evidence was trusted beyond the hop it measured.** Local ACK progress is
strong evidence about the Dell-to-relay hop and none about the relay-to-YouTube
hop, which is the hop the backup protects. Step 5 used it as general
liveness.

**Bugs sat in transitions, not states.** Steps 4 and 7 were both about what
happens when the classification changes: a new FFmpeg generation resets a
baseline, and leaving a clock hold reuses an old timer. Each state alone
behaved as specified.

**Two kinds of checking found different gaps.** The discarded copy facts and
the 75 s no-start were found by running the actual controller main loop, real
FFmpeg, the real nonce exchange and kernel TCP counters in isolated namespaces,
with faults injected into owned processes. The ACK-scope gap and the expired
timer were found by review: tracing the decision code against fault classes the
tests had not yet included, then confirming each with a deterministic main-loop
trace before changing anything.

## What Was Kept

Failed attempts are part of the record. The 75 s no-start result, the 600 s
trace and the immediate-start trace are each kept as regressions and in the
dated records below. Results from an earlier revision are not presented as
evidence for a later one. The October 20 production drill is labelled as
former-policy evidence wherever it appears.

## Still Open

The detection policy is verified in isolation and deployed. Production fault
injection, YouTube's selection of the backup ingest, uninterrupted public
playback and real alert delivery are not. A cold backup still interrupts local
reception for about 48-49 s.

## Dated Records

| Steps | Record |
| --- | --- |
| 1-2 | [Clock skew and bounded recording](fallback-clock-recording.md), [clock contract](cross-host-observation-time.md) |
| 3 | [Causal guard](route-causality.md), [fault and player validation](route-failover-validation.md) |
| 4 | [Initial relay-stop validation](relay-stop-main-loop.md) |
| 5 | [Relay stop across FFmpeg generations](relay-recovery-deadline.md) |
| 6-7 | [Upstream ACKs versus YouTube confirmation](youtube-egress-failover.md) |
| 8 | [Clock labels, missing samples and bound media progress](clock-safe-media-progress.md) |
| 9 | [Boottime freshness and independent UTC diagnostics](boottime-media-health.md) |
