# Isolated real-systemd reception tests

`launch.py` starts a real systemd as PID 1 in new user, PID, mount, network,
IPC, UTS and cgroup namespaces. It uses the public guard's actual LiveBackend,
fixed systemctl command and durable store. The receiver is a native C fixture;
readsb is a separate synthetic producer service. No USB or RF hardware is used.

The nested manager gets a minimal root, its own bus, machine ID, `/run`,
`/var`, journal and state. `pivot_root` detaches the old root. No host home,
receiver state, control socket, USB device or network interface is passed.
Only the dedicated test unit's case cgroup is writable. Isolation is checked
before testing. The source service hardening is retained; default dependencies
are disabled for the minimal target and recovery is explicitly enabled only
inside the isolated manager.

## Explicit local execution

Requires root, Linux cgroup v2 with delegation, util-linux `unshare`, Python,
a C compiler, `ip`, systemd and journald. Tested with systemd 259.5; other
versions and more restrictive namespace policies are unverified. Do not change
host security policies to make a failed probe pass. These commands create only
owned test services; stop tests if production observations change unexpectedly.
Keep all generated evidence **outside the public checkout**.

From the repository root, create a new private output directory:

```sh
repo_root="$(pwd)"
lab_evidence="$(mktemp -d /tmp/airspy-systemd-lab.XXXXXX)"
sudo systemd-run --unit=airspy-systemd-lab-demo-probe --collect --wait --pipe \
  -p Delegate=yes -p DelegateSubgroup=supervisor \
  -p CPUQuota=12% -p MemoryMax=256M -p TasksMax=64 \
  -p RuntimeMaxSec=1000 -p TimeoutStopSec=5 \
  /usr/bin/python3 "$repo_root/ops/airspy-reception/systemd_lab/launch.py" \
  "$lab_evidence" probe
```

Use a fresh unit name for each execution. The `airspy-systemd-lab-` prefix and
delegated `supervisor` subgroup are required by the launcher. Existing case
directories are refused rather than reset. After the probe passes, run a case
with the same resource properties and the relevant case name. Use `success
--full` for the default 60-second source and 30-second timer. Other cases use
shorter detection waits but retain the real 15-second systemctl timeout.

| Case | Actual service behavior exercised |
| --- | --- |
| `success` | Bad origin startup, one restart, positions and counter recover |
| `failed_reception` | Restarted process stays running but reception remains bad |
| `job_failure` | Restarted target exits during the real start job |
| `timeout` | Origin ignores SIGTERM; stop job lasts 30 seconds, client times out at 15 |
| `monitor_crash` | After reservation and accepted stop job, kill monitor and child command; resume with the same state |
| `gain_change` | Test-owned external controller changes synthetic gain 16 to 18, preserves the Bias-T option and restarts; the guard gets one extra restart |

The gain-change fixture alone mounts its own synthetic `/etc/default`
directory writable for the external controller. The guard never writes it.
These fixture values are not a recommended gain schedule or a power policy.

Each case requires one initial start, one external origin start and one
automatic start, one durable reservation, attempt count 1, the same origin
episode, and unchanged readsb PID/InvocationID. The monitor is then explicitly
started three more times and resumed on its timer; target starts must remain
three. Accelerated cases use a one-second cooldown so a long cooldown cannot
mask a broken restart cap.

After all six cases finish, independently check their journals and ledgers:

```sh
sudo python3 ops/airspy-reception/systemd_lab/verify.py "$lab_evidence"
```

The verifier does not import the guard. It checks three distinct receiver
InvocationIDs, starts 1/2/3 in journal, one reservation, one consumed attempt,
origin identity, timeout/kill evidence, source digest and empty cgroups.
It also accepts selected case names after the evidence argument.

The launcher retains raw exit status. A namespace poweroff may send SIGINT to
the outer unshare (`-2`); it is accepted only with a passing case and empty
cgroup. Launcher failure/timeout kills only the owned case subtree. After
completion, also confirm the transient unit and its cgroups were collected.
Preserve failed evidence privately; do not add output directories to Git.
