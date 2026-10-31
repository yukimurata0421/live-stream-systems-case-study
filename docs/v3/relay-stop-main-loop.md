# Initial Relay-Stop Validation

Dated record, October 31, 2026. This page is step 4 of the
[revision history](fallback-policy-revisions.md): the failed test that showed
the first causal policy could hide a real relay outage. The current rule is the
[fallback decision contract](fallback-decision-contract.md). The linked test
source has since been revised to require a bounded start under complete
observation loss.

Under the policy tested here, observation loss alone never started the backup.
The October 20 production relay-stop drill used the earlier policy; its
52-second startup is historical evidence for neither this policy nor the
current one.

The initial [main-loop regressions](../../tests/test_fallback_relay_stop_loop.py)
run the actual controller entry, an HTTP server returning 503, atomically
replaced transport files, cause persistence, status serialization and an owned
replica CLI adapter. Only external placement and the clock/sleep dependency
are mapped. The health parser, witness, decision and scale invocation execute.
They use controlled elapsed time and synthetic transport records, so their
timings describe policy behavior rather than measured recovery latency.

| HTTP 503 plus local evidence | Full main-loop result |
| --- | --- |
| Fresh FIFO recovery, stable FFmpeg generation and unchanged ACK counter | One start at logical 45 s: baseline at 0, first failure at 15, then 30 continuous seconds |
| Unchanged export, missing socket alone, or generation change on every observation | No start through logical 90 s; failure duration remains zero |
| One generation change followed by fresh stable FIFO recovery | Baseline resets, then one start at logical 60 s |

A generation change does not permanently prevent recovery. Repeated changes or
insufficient fresh failure evidence can repeatedly reset its confirmation window.
If neither relay facts nor local transport evidence can confirm failure, even a
genuine relay outage can leave backup off. This is the availability cost of
avoiding false starts from an observation outage.

## Native relay stop

Separate loopback network namespaces ran the installed controller and facts
collector, native MediaMTX and a local-output copy variant. Only the owned relay
process was stopped, after healthy input/output and local ACK progress were
established. The actual FFmpeg 5.1.9 executable and output/transport producer
modules were captured read-only from the operated runtime. Synthetic H.264/AAC
media was re-encoded at 320x180/10fps with one encoder thread. Real stderr, kernel
TCP counters and the producer's atomic transport export fed the unchanged
controller main loop; its 15-second poll and 30-second confirmation were real.

| Condition | Observed result |
| --- | --- |
| Operated FIFO options: recovery wait 0.2 s, maximum 20 attempts; owned sender restarted 5 s after exit | No backup within 75 s. FFmpeg exhausted retries and restarted six times; each sampled new generation required a baseline, so confirmed failure time remained zero. Recovery availability failed in this fixture. |
| Positive control: recovery wait extended to 5 s, maximum still 20 | Same FFmpeg generation stayed in FIFO recovery. One backup start 61.889 s after the relay-stop request; an independent local RTMP receiver accepted advancing backup bytes. |
| Observation-loss control: same long recovery, transport publication frozen before relay stop | No backup within 75 s; the unchanged export was rejected as stale. |

The positive control deliberately differs from the operated recovery wait. It
proves that the current main loop can start from genuine local failure evidence
while facts return HTTP 503; it does not re-establish the historical 52-second
result for current settings. One post-stop ACK increase was still observed before
the positive control accumulated its continuous failure window.

The five-second restart supervisor is a fixture, not the complete production
engine. This experiment demonstrates a failure path under repeated generation
changes; it does not determine every restart cadence or the outcome of a
production relay outage. A missing export after sender exit can also leave
failure unconfirmed. Reliable relay-stop recovery under current settings remains
an unresolved design issue. Runtime retries, thresholds and production policy
were not changed by this review.

All three native cases completed and all owned children exited. A five-second
read-only production guard recorded 23 healthy samples over about 112 s, stable
FFmpeg/connection generations, advancing ACKs and zero backup replicas. Private
HDD evidence preserves nine earlier case setups that failed before any relay
stop: FIFO/remux incompatibility in six and guard-directory permissions in three.
The final inputs re-encode like the runtime and verify guard readiness before
starting cases. No production fault or provider mutation was performed.

The initial public regression run passed 100 tests and 111 subtests, including
the three new main-loop tests. This is separate from native recovery availability.
The [sanitized validation record](../../publication/relay-stop-validation-20261031.json)
retains the aggregate outcomes; raw process/transport evidence stays private.

The [causal guard](route-causality.md) describes the cause ID and
secondary-recovery veto introduced with that policy. The separate [input-loss/player study](route-failover-validation.md)
measured local AV resumption with a long gap; it did not stop the relay and did
not prove continuous reception or actual YouTube backup selection.
