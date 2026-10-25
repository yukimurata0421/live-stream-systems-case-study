# Safety Model

The [current contract](local-reflex-authority.md) defines the restart conditions.
These are the boundaries the current code and tests preserve:

- Signed, monotonic policy versions select `off`, `dry_run` or `enforce`.
- Distinct fresh observations must show ACK stagnation and queued data. Missing
  data, target or connection changes reset the comparison. Network or ingest
  unreachability suppresses intervention.
- A confirmed stall waits through the engine grace window; an exiting or
  restart-waiting lifecycle is not an eligible running child.
- Persistent reservation precedes sending. The agent and Stream Executor fence
  the same generation, and cooldown/hour/day limits survive restarts. Rejections,
  unknown outcomes and cancelled reservations keep their consumption.
- Immediately before sending, the agent checks policy, STOP, target, route,
  Executor, authority, observation freshness and the three-second request expiry.
- An asynchronous acknowledgement is not completion. A lost reply is reconciled
  by querying the same request, never by sending another effect.
- Transport recovery and execution outcome are separate. A recovered stream
  does not turn an unknown execution into a success.
- The Executor's unresolved fence stays occupied until supported reconciliation.
- Audit is append-only. Central acknowledges its contiguous stored sequence with
  a signature; missing or conflicting records remain visible.

The physical [Stream boundary](../../src/runtime_boundary/) is retained unchanged.
Only it may act on the exact managed FFmpeg child. No CRA path requests a Pod,
container or host restart. `dry_run` and passing tests are not production authority.

See [test boundaries](harness-trust.md) and [operations](local-reflex-operations.md).
The older lease/epoch/command-outbox design and its guarantees are described in
[historical source](history.md), separately from this contract.
