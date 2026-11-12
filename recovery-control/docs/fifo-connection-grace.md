# Restart the stall grace window after a FIFO connection change

Evidence cutoff: November 13, 2026, 07:34:47 JST. This is a dated report from
retained operated evidence. Publishing this source does not deploy it or enable
production effects; the public policy examples remain `dry_run`.

## Failure and correction

FFmpeg's FIFO output can replace its TCP connection while keeping the same
FFmpeg generation, Pod and route. The previous legacy observation identity
did not include the FIFO connection metadata. If the new connection happened
to have the same cumulative ACK value, CRA could extend the old stall across
the switch and request a restart before the new connection had its own grace
window. A generation fence prevents duplicate effects, but does not correct
the timing of the first request.

[`Observation.transport_identity`](../src/cra/observe.py) now includes
`transport_connection_generation`, `socket_cookie` and `socket_inode` from the
same TCP source used for the observation. A change in any field resets the
stall streak, start time and comparison baseline. The existing dispatch guard
also uses this identity and blocks a connection change before or after the
durable reservation. A cancelled reservation still consumes its budget.

This is an internal identity change. The request remains
`runtime.typed_effect_request.v2`; policy thresholds, authority, durable
per-generation claims and uncertain-outcome handling are unchanged. A legacy
publisher with none of these metadata fields retains its previous behavior.
Metadata disappearance or reappearance resets the baseline. This does not add
socket uniqueness guarantees to publishers that lack connection metadata.

## Reproduce the regression checks

From `recovery-control`, after installing the documented test dependencies:

```sh
python -m pytest -q tests/reflex/test_reflex_fifo_connection_grace.py
```

The ten checks use a temporary DB, a fixture policy signing key, fake network
probes and a fake effect sender. They contact no production service.

| Case | Expected result |
| --- | --- |
| Equal ACK values; connection generation, cookie or inode changes | Reset the streak/start time in both legacy and bound input forms; no request at 88 seconds, first request at 90 seconds on the new connection |
| Switch before reservation | Block with `TARGET_CHANGED`; no reservation or request |
| Switch after reservation | No request; retain `CANCELLED_BEFORE_SEND` and its consumption |
| Old inputs without metadata | Preserve the existing grace behavior |
| Metadata disappears and returns | Do not reuse the earlier stall time |

In the earlier operated validation, the old source failed nine of these checks
and passed one; the corrected source and installed release each passed all ten.
The separate real-time check ran for 172.007 seconds: after an 80-second old
stall and a connection switch, the new connection had zero requests at 88
seconds and its first request at 90.000041 seconds. ACK counters stayed equal.
That check also used temporary state and fake probes/effects: physical effects
and real network probes were both zero. These timings are retained results,
not a new measurement performed by this public port.

## Operated checkpoint

The corrected agent was installed at 07:22:52 JST as an immutable release.
Only one of the 17 CRA source files changed. Policy v3 enforce, Executor
authority, the Agent DB instance, signed cached policy and existing action
contents were preserved. The central service, media image and monitoring
configuration did not require a deployment change.

During a 300.279-second follow-up, all 31 delivery-host samples were HEALTHY;
11 monitoring-host samples reported no issue. ACK increased by 158,959,745
bytes, with no new enforce request or unresolved execution. The same media Pod,
four containers and FFmpeg generation continued. The central verifier updated
naturally, audit sequences 3485 through 3488 reached Loki, and the final source
audit matched 29 producers and 22 entrypoints. Current Prometheus alerts were
zero at the final checkpoint.

The first post-deployment validator had a one-character transcription error in
an expected container ID. It was corrected to read the retained raw baseline;
the service operation was not repeated. Failed validator output, the old source
comparison and historical action consumption were retained.

No new production FIFO connection switch had occurred after the fix by this
checkpoint. Correct switch handling and grace timing were established in the
isolated checks; the natural follow-up establishes the later operating state.
It does not establish a real CRA fault intervention or public-player AV
continuity. The existing central verifier alone did not detect the original
connection-boundary defect.

The [public port record](../../publication/cra-fifo-grace-port-20261113.json)
records source digests and validation performed for this publication. Private
state, raw logs, host bindings, keys and operational backups are excluded.
