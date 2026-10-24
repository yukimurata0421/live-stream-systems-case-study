# Isolated FFmpeg Restart And FIFO Measurements

These Linux tools compare process replacement with FFmpeg's internal FIFO
reconnection. They preserve protocol timing, retry phase, finite limits and
cleanup evidence. The [case study](../../docs/v3/ffmpeg-fifo-measurements.md)
contains the dated results and rollout failure; the
[runtime contract](../../docs/v3/ffmpeg-fifo-recovery.md) describes the application.

## Start Without A Live Host

```bash
python3 ops/fifo-lab/summarize.py examples/fifo-recovery/measurements-20261024.json
python3 -m pytest -q tests/test_fifo_transport_telemetry.py \
  tests/test_fifo_runtime_integration.py tests/test_fifo_target_snapshot.py \
  tests/test_fifo_lab_tools.py
```

The summarizer recomputes mean, sample SD, range and restore-phase counts from
57 curated numeric records. It needs only Python's standard library and does
not contact a host. Tests require the public development dependencies and the
local `recovery-control` package. The optional AV file test needs FFmpeg/ffprobe;
the optional C build test needs a C compiler. Neither test uses NVENC or a live
stream.

## What Each Tool Measures

| Tool | Measurement |
| --- | --- |
| `run.py` | Lightweight direct/FIFO comparison, isolated address change, packet blackout and decoded-frame counter. Includes a five-second restart control. |
| `restart_timing.py` | Separate stop/start delay, reader buffering and GOP controls. |
| `transport_timing.py` | Controlled RST; TCP, TLS and RTMP publish response, with no decoder. |
| `nvenc_transport_timing.py` | Same protocol comparison with synthetic 1080p/5fps NVENC p4 CBR4Mbps and AAC192kbps. |
| `stall_fifo_timing.py` | Drop only the old TCP connection, detect the stall, validate ownership and cut that socket once; keep the FFmpeg process. |
| `wan_fifo_timing.py` | Drop all remote experiment packets and change the isolated outer address; let FIFO recover itself. |
| `wan_ffmpeg_timing.py` | Same outage, direct FLV and natural FFmpeg exit followed by bounded restart, with initial wait zero. |
| `wan_repeated_timing.py` | Repeat 12/30/60 s outages for both modes; record phase and successful-attempt timing. |
| `wan_tuning_timing.py` | Three profiles, three outage lengths and three repetitions; inspect retry/timeout/GOP sensitivity. |
| `candidate_output_timing.py` | Candidate **output builder** and transport observer on synthetic AV; not qualification of a complete production capture command. |

`transport_probe.c` observes sender-only libc/GnuTLS boundaries and RTMP publish
acknowledgements without logging media payloads. It is preloaded into the
experiment sender only. The historical binary was FFmpeg 5.1.9 with GnuTLS;
other builds/backends need separate validation, not silent reuse of its results.

## Live-Lab Requirements

A real experiment requires two explicitly selected Linux lab hosts, passwordless
SSH, noninteractive permission for the listed transient systemd units, Python 3,
WireGuard/iproute2/nftables/OpenSSL/util-linux, MediaMTX and prepared FFmpeg
runtimes. `--host` and `--mediamtx` are required; there is no production host
alias or deployment path default. The logical `dell`/`oci` role names in events
mean sender/receiver and do not select physical hosts.

The controller creates one `PrivateNetwork=yes` unit per host. Each node rejects
the host network namespace and nonempty network setup before creating a test
TUN and WireGuard interface. Documentation-reserved addresses are used only in
those namespaces. The controller carries test packets over SSH; it can drop
them without changing a host route, firewall, ISP session or real public IP.
That SSH/TCP transport is a limitation of the experiment.

Stage a unique absolute lab root on each host before running:

```text
sender lab root/                 receiver lab root/
  node.py                         node.py
  keys/                           keys/
  dell/                           oci/
  sender-runtime/                 decoder/
    ffmpeg                          usr/bin/ffmpeg
    lib/                            usr/lib/... (if required)
      ld-linux-x86-64.so.2
      <matching shared libraries>
  transport_probe.so
  gpu-lib/ (NVENC only)
  candidate/ (candidate-output check only)
```

Use a reviewed, versioned FFmpeg binary and **dereferenced matching libraries**,
record their hashes, and verify a cold start under UID/GID 65534. The sender
uses its copied dynamic loader and explicit library path. The receiver must
resolve its prepared runtime's dependencies on that lab host. Do not redistribute
runtime binaries or vendor GPU libraries with this source port. NVENC trials
require matching CUDA/encode libraries and the explicitly permitted GPU devices;
only one experiment encoder runs at a time.

Roots and role directories are root-owned experiment assets. Ensure the media
UID can traverse the runtime paths; only the owned role directory is writable.
`keys/` is an empty experiment-only directory bound onto `/etc/wireguard` inside
the unit, preserving the host's AppArmor policy and hiding its real keys.
`node.py` generates temporary test keys/certificates and removes private keys
during cleanup. No home directory, production credentials or production state
is passed to media children.

Build the probe into the sender's prepared root:

```bash
cc -std=c11 -shared -fPIC -O2 -Wall -Wextra -Werror \
  -o /var/tmp/fifo-sender-lab/transport_probe.so \
  ops/fifo-lab/transport_probe.c -ldl
```

For `candidate_output_timing.py`, stage the public `ffmpeg_args.py` and
`transport_telemetry.py` under `candidate/stream_core/engine/`, with empty package
initializers. This changes the output builder, not the whole synthetic input
construction. A separate full-command AV check is necessary before deployment;
that distinction caused the first operated audio regression.

Example using prepared, dedicated lab hosts:

```bash
python3 -B ops/fifo-lab/wan_repeated_timing.py \
  --host lab-receiver \
  --mediamtx /opt/fifo-lab/bin/mediamtx \
  --output /var/tmp/fifo-results/unique-attempt \
  --local-root /var/tmp/fifo-sender-lab \
  --remote-root /var/tmp/fifo-receiver-lab \
  --unit unique-fifo-comparison \
  --guard-observation /var/lib/lab-observer/runtime-observation.json
```

NVENC tools require an explicit read-only observation of the running stream
they protect. Its fields are `observed_at`, `ffmpeg_generation`,
`local_ffmpeg_pid`, `pod_uid`, `ffmpeg_running` and `tcp_metrics.bytes_acked`.
The guard checks identity, freshness, ACK progress and GPU limits; it stops new
experiment work on disagreement. This public port did not run those trials or
qualify a new host. Select and authorize actual lab hosts before execution.

## Limits, Pings And Evidence

Units retain finite CPU/memory/tasks/runtime limits, a closed device policy,
inaccessible control directories and cgroup cleanup. Media processes drop to
UID/GID 65534 with no inherited capability or secret environment. GPU tests
share a physical GPU if run beside a stream, so namespace isolation alone is
not a resource guarantee. Retain the production guard and stop conditions.

WAN trials create four virtual response points inside the experiment.
Stall trials use three explicit read-only host-route IPv4 targets plus the
isolated WireGuard peer. A private JSON supplies `gateway`, `external` and
`oci_public` for `--ping-targets`. Current cadence is **0.5 s per point**.
Historical numeric records used 1 s; changing the tool did not change those
records. ICMP recovery and FIFO success are not AV or public-player evidence.

TCP/TLS/publish timings use the sender's monotonic clock. Cross-host decoded
frame comparisons additionally record clock mapping uncertainty. Synthetic
frame counters in the lightweight profile distinguish fresh frames from old
queue contents; NVENC protocol trials have no decoder and make no first-frame
claim. Failed or incomplete attempts get separate output directories and must
not be silently removed from the history or counted as successful samples.

Retain source/binary manifests, events, results and owned-child cleanup locally.
Generated logs, keys, libraries, captures and machine configuration are not
public artifacts. Stop/collect only the experiment-owned units and processes.
