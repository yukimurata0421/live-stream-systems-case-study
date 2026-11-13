# NVIDIA Driver 48-Hour Smoke Pipeline

This pipeline observes an explicitly installed R580 driver while the normal
stream continues. It uses the existing NVIDIA, map, NVENC and local transport
signals; it does not start another encoder or GPU workload. A driver update
and any interruption needed to apply it remain separate operator actions.

The public port includes the collector, bounded systemd units, a one-time
post-maintenance starter, an interactive return companion, and offline tests.
It does not include an installed release, live configuration, credentials,
runtime databases or a completed physical 48-hour qualification. Source and
validation identities are in the [port record](../../publication/nvidia-smoke-port-20261106.json).

## Supported Runtime And Observation Contract

The current adapter targets a single NVIDIA GPU, Ubuntu's `nvidia-*-580`
packages, local K3s with `/usr/local/bin/kubectl`, and one `stream-v3-runtime`
Pod in namespace `stream-v3`. It expects the existing four-container runtime,
`runtime_readiness.py --check-only`, the map's `/render/status.json`, and the
read-only `/run/stream-v3-transport-export/ffmpeg-transport.json` export.
These are deployment prerequisites, not a portable GPU benchmark interface.

| Signal | Required evidence |
| --- | --- |
| Driver | GPU UUID, `nvidia-smi`, loaded `/sys/module/nvidia/version`, installed driver/DKMS/utilities/encode package versions and container-reported driver agree with the exact expected version |
| Runtime | Same machine, boot, GPU, driver and Deployment owner; qualify successor Pod/container/image/FFmpeg/asset epochs and retain every transition |
| Rendering | An NVIDIA Vulkan map browser and its GPU descendant using NVIDIA libraries; fresh render heartbeat and increasing render timestamp. SwiftShader alone does not qualify |
| NVENC | Existing readiness check confirms the live NVENC encoder and RTMP/RTMPS socket without writing its establishment marker |
| Transport | Fresh source timestamp; record connection/reconnect/FIFO-recovery/retry-failure/queue-overflow deltas. Allow a bounded outage while GPU, encoder and rendering remain valid, then require ACK recovery |
| Kernel | No new Xid, driver API mismatch, bus loss or initialization failure after the initial journal cursor |
| Collector | SHA256 binding of the collector, shared map/transport parsers and observation-time dependencies |

The window is fixed at **172800 seconds**. `CLOCK_BOOTTIME` measures elapsed
time on the same boot; timestamps are displayed in JST. A baseline starts the
window only after collection completes. Collection must finish within 55
seconds; gaps over 150 seconds, identity changes during collection, or a wall
clock/boottime discrepancy over five seconds make the result inconclusive.
Freshness uses the time each input is read.

The timer waits **60 seconds after the preceding invocation finishes**.
With 20 to 30 seconds for collection, observations are approximately
80 to 90 seconds apart. This avoids the old activation-based timer
occasionally reaching the collector's 60-second guard early and skipping a
cycle. The 150-second gap limit is unchanged.

Policy `driver_with_runtime_transitions_v3` separates routine network recovery
from driver failure. `transport_observation` records `HEALTHY`, `RECOVERING`
or `EXPIRED`, per-sample/cumulative counter deltas and recovery episode counts.
Socket absence, paused ACKs and FIFO recovery may continue for up to **300
seconds from the last healthy observation**. Repeated reconnects do not reset
that deadline. A valid readiness JSON with exit code 1 remains an observation;
the shared readiness helper's default behavior for other callers is unchanged.

GPU/NVENC, actual running containers and render/semantic/asset checks remain
mandatory for qualified runtime samples. Network-induced Pod/Deployment
readiness loss alone does not end the driver trial. Counter regression stays
inconclusive, an expired recovery fails, and an unresolved recovery at the fixed
48-hour endpoint is inconclusive. The trial is not extended to make it pass.
This observer-only deadline does not change any live recovery policy or budget.

## Pod Updates And Runtime Segments

A normal Pod update or container/encoder restart does not itself fail the
driver trial. The collector binds each Pod through its controlling ReplicaSet
to the same Deployment UID and node. During overlap it prefers a ready Pod
whose ReplicaSet matches the desired template, or keeps observing the ready
old Pod while its replacement starts. It never chooses an unrelated labelled
Pod or silently switches to another node or recreated Deployment.

Pod absence, startup and acquisition-time replacement enter `TRANSITION`;
a full successor enters `VERIFYING`. Two consecutive observations must confirm
the same new epoch with valid GPU/NVENC/render/asset/semantic checks and
advancing ACK/render timestamps before it becomes `STABLE`. New images,
container IDs, restarts, encoder generations, configuration references and
asset revisions create recorded segments rather than overwriting old identity.
Counter baselines reset only for a changed encoder process epoch; cumulative
event history is retained.

Host GPU/driver/package, boot/clock, kernel-error and sampling checks continue
during startup. Only explicitly observed lifecycle transitions permit missing
runtime/transport inputs for at most **300 seconds from the preceding healthy
observation**. Flapping Pods or an earlier network outage cannot renew that
deadline. API acquisition failure without rollout evidence remains unknown.
A foreign owner/node, GPU error, timeout or an unresolved transition at the
fixed 48-hour endpoint cannot pass.

The window remains 48 hours of operated driver observation and is never
extended for updates. `runtime_observation` exposes segment/transition counts,
accepted identity, `validated_runtime_sec` and `runtime_unverified_sec`.
Bounded startup time is recorded as unverified workload time; a pass does not
claim 48 uninterrupted hours of encoding/rendering. `target` preserves the
initial identity, while `current_target` and transition samples describe the
validated successors.

An explicit `upgrade-observer` command supports only the reviewed v2-to-v3
collector handoff. It requires a running, fresh, healthy, unexpired v2 run,
exact old/new source digests and the same live Pod/encoder/assets. The real
writer appends an upgrade sample and source lineage without changing the run
ID, original timestamps, previous samples, counters or fixed deadline. Ordinary
ticks still reject unexplained collector changes. Failed/stopped/unknown runs,
wrong digests and replayed upgrades are rejected; they cannot be resumed or
refunded through this command. This is an explicit operator source update,
not an automatic source-admission bypass.

The GPU-only probe skips weather requests and tile verification and reads at
most the last 512 KiB of the browser log. It preserves the public map probe's
portable repository/state defaults and its existing SwiftShader observation
support. Only the smoke verdict requires actual NVIDIA rendering.

## Explicit Installation Boundary

Use a new root-owned release directory under
`/opt/stream-v3-nvidia-smoke/releases/`, preserving these repository-relative
paths. Point `/opt/stream-v3-nvidia-smoke/current` to that reviewed release.

```text
ops/host-maintenance/bin/nvidia_driver_smoke.py
ops/host-maintenance/bin/nvidia_smoke_after_maintenance.py
ops/host-maintenance/bin/nvidia_smoke_finish_maintenance.py
ops/scripts/stream_v3_map_runtime_probe.py
ops/scripts/stream_v3_transport_recorder.py
src/stream_core/common/observation_time.py
src/stream_core/common/__init__.py
```

The matching four service/timer definitions are under
`ops/host-maintenance/systemd/`. Install them in `/etc/systemd/system/` only
as an explicit host operation. Repository tests and CI never install units,
reload systemd, contact providers or change the running stream. Do not copy a
whole development checkout or overwrite an existing release or state store.

The collector service needs access to the local K3s kubeconfig and kernel
journal. Its examples restrict writes to its own state directory, API traffic
to localhost, and resources to CPU 10%, memory 192 MiB and 32 tasks, with
Nice 10 and a 100-second timeout. These host-service limits do not constrain
the short `kubectl exec` readers inside the stream container's cgroup.
The starter additionally needs local systemd timer control. Neither unit is
enabled by this source port.

## Start, Inspect And Stop

After an operator has installed and checked the expected driver and reviewed
the runtime bindings, use the installed collector. Version and run ID below
are examples, not evidence of an applied update.

```bash
runner=/opt/stream-v3-nvidia-smoke/current/ops/host-maintenance/bin/nvidia_driver_smoke.py
sudo /usr/bin/python3 -I -B "$runner" check --expected-driver 580.178.04
sudo /usr/bin/python3 -I -B "$runner" start --run-id r580-example --expected-driver 580.178.04
sudo systemctl enable --now stream-v3-nvidia-smoke.timer
sudo /usr/bin/python3 -I -B "$runner" status --run-id r580-example
```

`check` collects once without creating a campaign. `PRECHECK_PASSED` is not a
48-hour pass. `start` records the baseline; the timer then calls `tick`.
For an explicit stop:

```bash
sudo /usr/bin/python3 -I -B "$runner" stop --run-id r580-example
```

Stopping the campaign does not signal the stream or change recovery policy.
Terminal campaigns remain in the database, and subsequent ticks return
`IDLE`; no new campaign starts automatically. To retire this observation
installation, stop the owned campaign and disable only its smoke timers.
Keep its releases and evidence; leave delivery and recovery services running.

## Optional One-Time Maintenance Handoff

The starter binds a single planned maintenance session/generation, machine,
collector hash, run ID and expected driver. It requires that same session's
recorded operator confirmation for YouTube, Twitch and live-data updates,
followed by `NORMAL` and the expected loaded driver. An old `NORMAL` state
cannot start it. It does not perform driver updates or maintenance entry/exit.

During the explicitly entered [manual maintenance](manual-maintenance.md)
session, before returning to normal delivery:

```bash
starter=/opt/stream-v3-nvidia-smoke/current/ops/host-maintenance/bin/nvidia_smoke_after_maintenance.py
sudo /usr/bin/python3 -I -B "$starter" arm --run-id r580-example --expected-driver 580.178.04
sudo systemctl enable --now stream-v3-nvidia-smoke-launch.timer
```

The starter persists intent before calling the collector and reconciles its
SQLite result after a crash or timeout. Unknown start or timer outcomes are
not resent. Existing launch state cannot be rearmed or deleted to reset the
attempt. Once `STARTED`, disable the launch timer; leave the sampling timer
running and retain the launch history.

The optional `nvidia_smoke_finish_maintenance.py` companion uses the same
public maintenance core's `off` state machine but continues its existing
verification samples while the operator inspects the players. It preserves
the 300-second stability window, 45-second sample gap, authority/connection
checks, guard, shared lock and original recovery-timer restoration.

It requires the exact four root-owned public maintenance files listed in its
`PINS` under `/opt/stream-v3-manual-maintenance/current`, a real root terminal,
the armed session, and phase `VERIFYING` or `RESTORING`. After the existing
`stream-v3-maintenance resume` reaches that phase, invoke:

```bash
sudo /usr/bin/python3 -I -B \
  /opt/stream-v3-nvidia-smoke/current/ops/host-maintenance/bin/nvidia_smoke_finish_maintenance.py
```

Inspect both public players and live data before answering `yes`. No, EOF,
AV/output stall or authority drift retains inhibition. The wrapper does not
automatically attest player health or issue a new resume effect. Changed core
bytes fail the pins; rebuilding those pins requires reviewing and testing the
new core. It does not import an installed production release in offline tests.

## Evidence, Verdicts And Limits

`/var/lib/stream-v3-nvidia-smoke/smoke.sqlite3` contains `runs` and `samples`.
An actual SQLite transaction serializes writers through collection and commit.
A concurrent `status` reads committed data; competing `start`, `tick` or
`stop` operations fail busy without overwriting the run. There is one active
campaign, no run-ID reuse, no automatic deletion, no evidence refund and no
automatic extension of the 48-hour window. The database limit is 64 MiB.

| Verdict | Meaning |
| --- | --- |
| `RUNNING` | GPU/render/NVENC observations accumulate; transport may be healthy or within bounded recovery. Not yet a 48-hour pass |
| `PASSED` | At least 48 hours of the same host/GPU/driver, bounded transitions, and a final stable validated runtime with healthy transport |
| `FAILED` | A required GPU/runtime/render/transport condition failed; the driver is not automatically assigned as the cause |
| `INCONCLUSIVE` | Missing/stale input, clock/boot change or acquisition uncertainty |
| `STOPPED` | Explicit operator stop |

`status` is read-only. A stale saved `RUNNING` reports an inconclusive verdict;
the next tick persists termination. `SuccessExitStatus=1` prevents a recorded
test failure from being confused with a systemd invocation failure; it does
not make that test pass. A real recovery is allowed to invalidate the smoke
identity: the observer never blocks delivery recovery.

Raw command lines, Kubernetes objects, credentials and kernel messages are not
stored. `provider_receiver_verified=false` is retained. Local ACKs, render
progress and public-player operator attestations remain separate evidence.
Events that occur and recover between samples without changing a persistent
counter cannot be ruled out.

## Offline Verification

```bash
python3 -m pytest -q \
  tests/test_nvidia_driver_smoke.py \
  tests/test_nvidia_smoke_after_maintenance.py \
  tests/test_nvidia_smoke_finish_maintenance.py \
  tests/test_map_gpu_observation.py \
  tests/test_stream_v3_map_runtime_probe.py \
  tests/test_transport_recorder.py \
  tests/test_manual_maintenance.py
```

Tests replace external command/file I/O, retain the real parsers and SQLite
writer/reader, simulate the fixed 48-hour clock, reproduce the timer boundary,
and exercise two real concurrent processes. They also cover unknown handoff
outcomes, confirmation delays and rejection paths, actual public-core hash
binding, and cold installed-layout imports without a development checkout.
The simulated 48-hour run includes recoverable network/socket/readiness loss
and Pod replacements on both days. Persistent failures, repeated reconnects that cannot reset the
deadline, counter regressions, unresolved recovery at the endpoint and GPU or
render failure during recovery are negative cases. Tests also cover owner/node
rejection, overlapping old/new Pods, startup, image/container/encoder updates,
fixed-endpoint rollout rejection and a real SQLite observer handoff preserving
the sample prefix and original start time. These checks do not exercise
a physical GPU for 48 hours or confirm receivers.
