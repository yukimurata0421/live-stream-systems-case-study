# Measuring FIFO Recovery Before Replacing FFmpeg

Measured: **2026-10-24 JST**. This is a dated engineering case, not a current
health report. The [runtime contract](ffmpeg-fifo-recovery.md) explains why
reconnection was chosen; the [isolated lab](../../ops/fifo-lab/README.md)
produced the curated
[timing records](../../examples/fifo-recovery/measurements-20261024.json).
Private logs, network addresses, media and process/socket identities are excluded.

## Findings

- **FIFO was not consistently faster than restarting FFmpeg.** Group means
  differed by about 0.4 s or less, in both directions. The restart path
  omitted its production five-second initial delay.
- **Waiting, not connecting, dominated.** TCP completion to publish
  acknowledgement took about 0.4 s in every group. The variation came from
  when the successful attempt began relative to link restoration.
- **No trial measured riding through.** The receiver read timeout was shorter
  than every outage, and every trial ended on a new connection. Riding through
  was [deferred by design](ffmpeg-fifo-recovery.md#choosing-the-recovery-layer).
- **Shorter timeouts helped the 12-second case by failing earlier.** The tuned
  profile changed three settings together, so no single setting is credited.
- **The first production rollout lost audio for about 12 minutes** because a
  full-command test was missing. The corrected exercise reconnected in the same
  process in about 1.95 s with both tracks returned.

## Question And Method

Would reopening only the output materially reduce recovery time compared with
restarting FFmpeg? Both options reconnect; the comparison does not include
keeping the original connection open.

Media used synthetic 1080p/5fps video, NVENC p4 CBR 4 Mbps and AAC
192 kbps/48 kHz/stereo, matching the recorded production encoding profile.
Production X11/Pulse inputs were not used in the comparison.

Two temporary PrivateNetwork units contained WireGuard, sender/receiver
processes and test-only routes. An existing SSH connection carried their IP
packets. The experiment dropped those packets and changed the sender's isolated
outer address. It did not reconnect an ISP session or change a real public IP,
and SSH/TCP behavior remains part of the test link. These trials sampled
reachability with one-second pings; the current tools use 0.5 s, and the old
results were not relabelled or rerun.

The restart supervisor omitted the initial five-second delay. FIFO kept the
same process; direct FLV exited and was recreated, with a one-second wait
between failed attempts. Both paths had finite retry limits. Retry phase was
left natural rather than aligned to the moment the link returned.

The endpoint was the sender receiving RTMP `NetStream.Publish.Start`. A
sender-only libc/GnuTLS probe separated TCP connect, TLS handshake and publish
response on the same monotonic clock. These NVENC trials checked both AV tracks
and increasing receiver bytes but started no decoder.

## Restart Versus FIFO

Seconds from simulated link restoration to publish acknowledgement.
Each cell is mean ± **sample** standard deviation; five trials per method and
outage length, 30 completed cases. Sender protocol timeout 15 s, FIFO retry 1 s,
receiver read timeout 8 s, GOP 10.

| Outage | FFmpeg restart | FIFO reconnect |
| --- | ---: | ---: |
| 12 s | 5.479 ± 1.387 | 5.218 ± 1.249 |
| 30 s | 1.619 ± 0.095 | 1.249 ± 0.132 |
| 60 s | 0.973 ± 0.863 | 1.394 ± 0.170 |

### Where The Time Went

Means per group. "Wait" runs from restoration to the start of the successful
TCP attempt; an attempt already in flight at restoration has no wait.

| Outage | State at restoration (restart / FIFO) | Wait (restart / FIFO) | TCP complete → publish (restart / FIFO) |
| --- | --- | ---: | ---: |
| 12 s | Original connection not yet failed: 5 / 5 | 4.98 / 4.78 | 0.46 / 0.40 |
| 30 s | Original connection error still being handled: 5 / 5 | 1.19 / 0.78 | 0.41 / 0.44 |
| 60 s | Restart: successful attempt in flight 4, earlier attempt in flight 1. FIFO: between attempts 4, earlier attempt in flight 1 | mostly in flight / 0.97 | 0.39 / 0.39 |

At 12 s the original connection was still open when the link returned, yet it
did not resume; recovery waited for that connection to fail and a new attempt
to begin. At 60 s, restart trials usually had an attempt already under way,
while FIFO trials waited for the next one. The 60-second result reflects retry
phase, not an inherent advantage of process restarts for long outages. TLS
and RTMP portions are retained separately in the records.

## Tuning Was A Separate Experiment

Three trials per profile and outage, 27 completed cases, FIFO only:

| Outage | Baseline | Tuned | Tuned plus GOP 5 |
| --- | ---: | ---: | ---: |
| 12 s | 4.923 ± 1.025 | 1.244 ± 0.188 | 0.951 ± 0.477 |
| 30 s | 1.241 ± 0.123 | 1.251 ± 0.141 | 1.225 ± 0.064 |
| 60 s | 1.406 ± 0.200 | 0.950 ± 0.374 | 0.669 ± 0.284 |

The tuned profile changed FIFO retry from 1 to 0.2 s, inner timeout from 15 to
3 s and receiver read timeout from 8 to 4 s together. The third profile also
changed GOP 10 to 5. Every tuned trial already had its successful attempt in
flight at restoration. The production adoption used the tuned FIFO retry and
inner timeout, kept NVENC, AAC and GOP 10, and left the relay's existing
timeout unchanged.

## The First Rollout Lost Audio

The first production FIFO rollout lost its audio track at the relay. The
isolated sender had mapped audio explicitly, and the candidate check replaced
only its output builder. Production used a different complete command with an
unnamed complex-filter output and automatic stream selection. Matching encoder
settings and a working output builder had not tested the full invocation.

An independent observer also recognized only direct `-f flv` and rejected the
FIFO process. Local ACK progress and forwarding bytes had established neither
audio health nor a valid target identity.

The original template was restored. Audio-track absence was observed at the
relay from about **12:06 to 12:18 JST**. Viewer impact at each provider was not
measured, and use of the YouTube backup does not show that viewers were
unaffected. A same-PID reconnect about 1.35 s after planned restoration was
retained but not counted as a successful rollout.

The correction named and explicitly mapped the filtered video and Pulse audio,
and made the target classifier FIFO-aware without relaxing ownership or identity
checks. A three-second local file used the final full builder with actual
X11/Pulse inputs and NVENC/AAC; both tracks and audio decoding were checked
before the corrected rollout. The public regression exercises the full output
command on synthetic CPU-encoded AV.

## Corrected Production Exercise

One 12-second, kernel-expiring rule dropped only the relay peer's outer
WireGuard UDP traffic. It changed neither the real public IPv4 nor the ISP
session. Four pings recorded gateway, an external endpoint, relay-public and
relay-tunnel reachability.

| Observation | Result |
| --- | --- |
| Process and Pod identity | Unchanged. |
| Observed connection generation | 1 → 2; one FIFO reopen report. |
| Reconnect timing | About **1.95 s** from planned restoration to the trial collector observing ACK progress on the new connection; runtime journal about 1.83 s. |
| Observation resolution | About 1.1 s plus rule-application timing. |
| Retry reports | One failed attempt, then successful recovery in the same process. |
| Media verification | Returned H.264 and AAC; two-second audio decode; advancing rendered clock. |
| Forward verification | Relay input and YouTube/Twitch forward bytes increased. |
| Cleanup | Owned fault rules and pings removed; the normal controller resumed and stopped the temporary backup after sustained health. |

Gateway and external ICMP continued, and the tunnel reply gap was about
12.32 s. The relay-public endpoint did not answer even before the fault, so it
could not establish recovery. Ping success was never the media-success gate.

## What These Results Do Not Show

- The behavior of a real ISP session change or public IP change.
- First-frame decode, provider latency, public-player continuity or viewer impact.
- A guaranteed recovery deadline, or the effect of any single tuned setting.
- Long-term queue and timestamp behavior after reconnection.
- The full sequence of retry exhaustion, FFmpeg exit, replacement and
  successful reception.
- Whether riding through the gap would have been faster; no trial kept the
  original connection.

## Recompute Without A Live System

```bash
python3 ops/fifo-lab/summarize.py \
  examples/fifo-recovery/measurements-20261024.json
```

The output reproduces the [published group statistics](../../examples/fifo-recovery/summary-20261024.json)
from 57 curated numeric records. Initial setup failures, an incomplete collector
attempt and the failed first production rollout remain separate history; they
are not converted into completed trials. The command performs no network
experiment or deployment.

The [public source port record](../../publication/fifo-recovery-port-20261024.json)
lists the ported files, adaptations, passed checks and retained failed checks.
Those checks validate the public source, not a new production recovery campaign.
