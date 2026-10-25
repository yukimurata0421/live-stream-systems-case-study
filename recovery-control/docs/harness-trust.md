# Test Boundaries

Current CRA tests live in `tests/reflex/`. They use temporary databases and keys,
injected clocks and observations, loopback sockets and test-owned effect callbacks.
They do not use production credentials, delivery targets or a private checkout.

Unit tests cover policy signatures and rollback rejection, ACK-stall criteria,
the engine grace window, lifecycle guards, persistent budget consumption,
STOP/policy changes before dispatch, stale observations, audit acknowledgements,
lost replies, unknown outcomes and recovery-evidence verification.

Integration tests use the real Stream [Effect Executor](../../src/runtime_boundary/)
and its ledger, not a replacement for the admission or fence logic. They exercise
v2 request compatibility, asynchronous completion and lost replies, unresolved
fences, authority handover and rollback. Only the physical callback is replaced
with an effect recorded in a test-owned list. No real FFmpeg is signalled.

Stream's own [boundary tests](../../tests/runtime_boundary/) retain protocol,
expiry, target drift, concurrency, child registration, route fences and recovery
publication checks. They run in the Stream suite. The public CI runs both suites.

These checks establish local code behavior and interoperability. They do not
establish deployment, real recovery benefit, receiver continuity or a completed
production soak. The [dated records](local-reflex-records.md) keep those evidence
levels separate. The general three-host chaos harness and fixed-SQLite soak
qualification belonged to the [previous design](history.md); they are not an
installation prerequisite for the current CRA.
