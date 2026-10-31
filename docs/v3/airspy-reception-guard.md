# One bounded restart for an Airspy reception collapse

The optional [receiver-host module](../../ops/airspy-reception/README.md)
detects a specific startup failure: Airspy and readsb processes are running,
their JSON remains fresh, yet decoded traffic collapses and positioned aircraft
stay at zero. It can restart Airspy once without restarting the stream.

This public port adds source, inert installation examples and tests. It does
not deploy the public example, change operated gain or Bias-T power, enable
another recovery writer or send notifications.

## Incident and choice of boundary

On 2026-10-30 JST, a receiver startup associated with the existing gain
controller was followed by sustained zero positioned aircraft. An Airspy-only
restart with the same configuration restored reception. The decoder and media
delivery remained running. This narrows the fault to receiver-side state but
does not distinguish software/device initialization from Bias-T/LNA behavior.
Gain was not proven to be the cause, and power was not measured.

The guard therefore owns only startup reception verification and one extra
Airspy restart. The existing gain controller owns scheduled gain transitions;
the media renderer, FFmpeg and stream recovery authority remain separate.
No receiver configuration writer, personal time schedule, account notification
adapter or private deployment installer is shipped.

## Policy and persisted uncertainty

A valid pre-start baseline, fresh and bound source data, zero positions,
stalled position counter, rate below the configured ratio and a sustained
multi-snapshot failure are all required. Process liveness alone is insufficient.
The configurable 10% ratio is provisional: a single recorded incident stayed
below 8.58%, versus comparison-morning minima of at least 15.97% in seven
observations. This small sample does not establish a false-positive rate or
generalize to other antennas, RF environments or metric implementations.

The guard fsyncs attempt 1 before calling the fixed Airspy service restart.
A retry PID change, nonzero command result, 15-second client timeout or monitor
death cannot rearm that origin. An accepted systemd job may complete after
the client disappears; its later InvocationID remains attached to the original
episode. Missing/corrupt state or binding drift blocks effects instead of
recreating a budget. Threshold changes retain the consumed attempt.

An external gain transition is another startup with the same one-extra-attempt
limit. Only a verified recovered episode followed by an external startup can
establish a fresh budget; unresolved outcomes are handled conservatively.

## Validation boundaries

The public port preserves the AST of all 11 shared decision, store and adapter
functions/classes. English prose replaces private comments. The personal
gain-writing function and its CLI entry are removed. The public service starts
in `observe-only`; a separately supplied drop-in explicitly opts into effects.

Portable tests exercise distinct timestamps, source binding, durable state,
concurrent CLI processes, real flock/fsync, child exit, storage write failure,
timeout and external gain transitions. Their backend effects are synthetic
and are not by themselves service-manager evidence.

The [integration runner](../../ops/airspy-reception/systemd_lab/README.md)
connects the exact public source to a separate real systemd PID 1 and actual
services. The success case uses default policy, a 60-second source and a
30-second timer. Failure and external-gain cases shorten detection waits but
retain the 15-second command timeout and durable budget. All receiver data is
synthetic; public CI runs only the portable tests and boundary checks.

The dated results and exact public digests are recorded in the
[publication record](../../publication/airspy-reception-port-20261030.json).
Generated journals, state, live snapshots and raw operational evidence remain
private. A real-service test with a synthetic receiver does not establish
physical RF recovery, a production fault-triggered automatic restart,
Bias-T voltage behavior or public-player reception.

On 2026-10-30 JST, the public source passed 34 receiver tests; the combined
receiver/documentation/public-boundary run passed 64 tests and 1,017 subtests.
All six real-systemd cases passed independent journal/ledger verification:
success, persistent reception failure, restart-job failure, client timeout,
monitor kill/restart and an external synthetic gain transition. Each recorded
one automatic restart and zero further starts after three explicit monitor
restarts and resumed timer observations. All test cgroups were empty and
collected afterward. The final account-independent home-isolation assertion
was also exercised by a fresh real-manager probe.

The source port was performed while operated receiver, gain/power settings and
delivery were preserved. [Current status](../current-status.md) separates
operated deployment, isolated validation and unobserved outcomes.
