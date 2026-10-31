# Optional Airspy reception guard

Detect a sustained reception collapse after a receiver startup and allow at
most **one additional Airspy restart for that origin startup**. A startup
performed by an existing gain controller is an external startup too. The guard
does not choose gain values, schedule gain changes or control Bias-T power.

This is an optional receiver-host tool, separate from the stream renderer,
encoder, relay and stream recovery controllers. Nothing in the stream package,
k3s manifests or public CI installs or enables it. The supplied service runs in
**observation-only mode** until an operator explicitly opts into recovery.

## Input and action contract

The adapter deliberately fixes the receiver target and metric paths:

| Input | Required content |
| --- | --- |
| `airspy_adsb.service` | Active/running, MainPID, InvocationID and monotonic start time |
| `/usr/local/bin/airspy_adsb` | Exact executable of MainPID; binary and argv binding |
| `/etc/default/airspy_adsb` | Exactly one numeric `GAIN`, matching reported gain |
| `/run/airspy_adsb/stats.json` | `now`, `gain`, `lost_buffers`, 32 nonnegative `df_counts` |
| `/run/readsb/stats.json` | `now`, `total.start`, `total.position_count_total` |
| `/run/readsb/aircraft.json` | `now`, aircraft `lat`, `lon`, `seen_pos` |

The rate is `sum(df_counts) / 60`; input counters must describe a 60-second
interval. Inputs with different schemas need a separately reviewed adapter;
their absence is UNKNOWN, not zero traffic. The adapter does not claim support
for every upstream Airspy/readsb version.

The sole recovery command is `/usr/bin/systemctl restart airspy_adsb.service`,
with a 15-second client timeout. The environment cannot substitute a target or
command or raise the restart cap. The guard never restarts readsb, the stream,
USB, containers or the host, and never writes receiver configuration.

## Detection and durable budget

Defaults in [the environment template](airspy-reception.env.example):

- A 300-second baseline needs three distinct snapshots, at least two fresh
  positioned aircraft in every retained baseline sample, and position-counter
  progress from the same decoder instance.
- After 90 seconds of startup grace, zero fresh positions, no position-counter
  progress, and a rate below 10% of baseline must persist for 180 seconds over
  at least three distinct source timestamps.
- Recovery also requires fresh sources, stable identity/config/binary/argv,
  zero lost buffers, startup age at most 900 seconds and a 300-second cooldown.
- Immediately before the action, all bindings and recovery signals are
  checked again. Attempt 1 is atomically saved and fsynced before systemctl.

The cap is per origin episode, not per monitor process or receiver PID. Retry
children, failed commands, timeout, monitor death and unknown outcomes retain
the consumed attempt. A later external startup, including a gain transition,
can have its own budget after verified recovery. An unresolved outcome or
binding drift remains conservative and cannot mint a new attempt.

Missing/corrupt state fails closed; observation never initializes it. Preserve
the whole state directory across upgrades, timer restarts and policy changes.
There is no reset-budget command. Do not delete it to retry a failed action.
Confirmed recovery requires two distinct post-start snapshots with fresh
positions and counter progress; a running process alone is insufficient.

The 10% ratio is configurable through `AIRSPY_RX_RATE_RATIO`. It is a
provisional threshold from one operated installation: its recorded collapse
stayed below 8.58% of baseline, while seven comparison mornings had minimum
ratios of 15.97% or more. That is a small historical sample, not a universal
RF threshold or a false-positive estimate. No historical raw rows are shipped.
The ratio cannot trigger a restart without the other conditions above.

## Deliberate receiver-host installation

Requirements: Linux, systemd, Python 3.11+, the input contract above, and an
operator who owns the receiver service. These are manual examples, not a
deployment action performed by this repository. Inspect existing services and
state first; do not overwrite an existing installation or its state.

From the repository root, for a **fresh** installation:

```sh
sudo install -d -m 0755 /opt/airspy-reception-guard/current
sudo install -m 0644 ops/airspy-reception/receiver_guard.py /opt/airspy-reception-guard/current/
sudo install -m 0644 ops/airspy-reception/airspy-reception.env.example /etc/airspy-reception-guard.env
sudo install -m 0644 ops/airspy-reception/airspy-reception-guard.service /etc/systemd/system/
sudo install -m 0644 ops/airspy-reception/airspy-reception-guard.timer /etc/systemd/system/
sudo install -d -m 0755 /var/lib/airspy-reception-guard
sudo python3 /opt/airspy-reception-guard/current/receiver_guard.py init
sudo systemctl daemon-reload
sudo systemctl enable --now airspy-reception-guard.timer
```

`init` refuses an existing state or initialization marker. The service's
`observe-only` mode reports `WOULD_RESTART` without consuming an attempt.
Check the inputs, bindings and policy before enabling effects. The timer runs
every 30 seconds; duplicate source snapshots do not advance persistence.

To explicitly opt in on that receiver host, install the supplied drop-in:

```sh
sudo install -d -m 0755 /etc/systemd/system/airspy-reception-guard.service.d
sudo install -m 0644 ops/airspy-reception/enable-recovery.conf.example /etc/systemd/system/airspy-reception-guard.service.d/enable-recovery.conf
sudo systemctl daemon-reload
```

The next timer invocation uses `observe`. This does not restart the receiver
immediately; all gates still apply. Do not use the guard as a second writer
alongside an existing recovery guard. An external gain controller continues
to own its gain/power policy and receiver startup. Never overlap its unresolved
restart with a second recovery request; reconcile observed state first.

To suspend recovery, stop this timer and wait for its oneshot service to
finish before removing only the opt-in drop-in. Keep state and events. A
client timeout or killed monitor does not cancel an accepted systemd job;
inspect the target and job before issuing any manual receiver action.

## Status, tests and publication boundary

```sh
cat /var/lib/airspy-reception-guard/status.json
systemctl status airspy-reception-guard.timer
journalctl -u airspy-reception-guard.service
python3 -m unittest discover -s tests -p test_airspy_reception_guard.py -q
```

`status.json` is a readable summary; state, initialization marker, lock and
events remain private. UNKNOWN, EXHAUSTED, RESTART_FAILED and OUTCOME_UNKNOWN
must not be reported as healthy. Notification adapters may read this status;
personal webhook configuration and existing notification integration are not
included.

[Real systemd integration tests](systemd_lab/README.md) use a separate PID 1,
real services and real systemctl with synthetic receiver data. They are an
explicit root operation, never an ordinary CI test. See the
[dated port and validation record](../../docs/v3/airspy-reception-guard.md).
Production settings, gain schedules, installers tied to private hosts,
historical fixtures, raw journals, state, logs and credentials are excluded.
