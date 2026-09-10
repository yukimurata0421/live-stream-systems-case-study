# Harness Trust Boundary

The harness is trusted only when SUT behavior, evidence completeness, oracle
independence, fault lifecycle, negative controls, production isolation, and
artifact integrity all pass in the same run.

```text
immutable scenario -> injector -> SUT -> observer -> append-only evidence
                                          |
immutable invariants ---------------------+-> independent oracle -> report
```

## Roles

- The SUT is the authority, protocol, Dell Agent, and their SQLite stores.
- The observer records rows, messages, counters, target identity, timestamps,
  and exits; it does not decide PASS.
- The oracle uses immutable scenario invariants and evidence. It does not
  import the SUT decision or state-transition implementation to calculate the
  expected result.
- The classifier keeps `SUT_FAILURE`, `HARNESS_FAILURE`, `MISSING_EVIDENCE`,
  `UNKNOWN_AMBIGUOUS`, and expected injected failures distinct.

## Evidence and faults

Events carry a run ID, scenario ID, producer, monotonic sequence, event ID, and
observation time. A freeze rejects later append, sequence gaps, duplicate IDs,
wrong run/scenario bindings, and out-of-window timestamps.

Every injected fault moves through requested, armed, triggered, and completed
states. A fault that was requested but never triggered is a harness failure,
not evidence that the SUT tolerated the fault.

## Negative controls

The negative-control adapter intentionally creates forbidden states while the
production adapter remains unavailable. The trust gate closes if the oracle
misses any required injected failure. Production signal, process, Pod,
Deployment, network, and credential-load counters must remain zero.

## Limits

Harness success does not prove real LAN failure handling, production
credentials, a real FFmpeg effect, or viewer recovery. Those require separately
authorized, source-bound evidence and must never be inferred from synthetic
results.
