# Transport Flight Recorder

The recorder keeps detailed transport evidence around events on the main
sender's FIFO output: queue growth, ACK stalls, reconnects and observation
gaps. It reads the existing one-second telemetry, holds the last 120 seconds
in memory, and writes the window around each event to SQLite. It records; it
does not decide.

It replaces the learned throughput baseline; the
[retirement decision](route-baseline-retirement-decision.md) explains why.
This page is the contract: inputs, what is saved, when, how rates and timing
are computed, isolation, and limits.

## Scope

| In scope | Out of scope |
| --- | --- |
| The main sender's FIFO transport snapshot and its event journal | The fallback sender, which has no such telemetry |
| Event windows and per-minute summaries in a local SQLite database | Alerts, notifications, dashboards, incident or SLO verdicts |
| Read-only reports and comparison against independently observed fault intervals | Restart, scale, signal or any other effect; provider or viewer reception |

The recorder has no network access, starts no external process, and has no
learned baseline. Its status always outputs `action_enabled=false`
and `delivery_verified=false`.

## Inputs

| Input | Use |
| --- | --- |
| `ffmpeg-transport.json` (`stream_v3.ffmpeg_transport.v1`) | State, process and connection generation, accumulated ACK bytes, FIFO counters, and the socket's `send_q`, `bytes_sent`, `bytes_acked`, `notsent`, `unacked`, `lastsnd_ms`, `rto_ms` |
| FIFO event journal (optional) | Transitions that can fall between two polls; read from the bounded tail |
| Clock observation (optional) | An existing NTP measurement; without it clock uncertainty stays `UNKNOWN` |

The snapshot is written by [TransportTelemetry](ffmpeg-fifo-recovery.md#observing-recovery-without-a-new-pid)
each time the runtime observation samples the socket, about every 1.1 seconds
with variable spacing. It exists only while FIFO recovery is enabled; with FIFO
disabled it stops updating and an old file may remain.

## What Is Saved

| Item | Behavior |
| --- | --- |
| Poll | Every 1 second; duplicates are identified by source time |
| Memory window | Last 120 seconds, at most 1,024 rows |
| Event window | 120 seconds before the first trigger and 180 seconds after the last one |
| Merging | A new trigger extends the open episode, up to 600 seconds after the first trigger; with the pre-window, one episode spans at most about 720 seconds |
| Always kept | Per-minute ACK and send Mbps, queue minimum, mean and maximum, and fresh, duplicate and unavailable counts |
| Storage | Single writer, SQLite WAL with `synchronous=FULL`, 256 MiB total budget |

An episode opened shortly after start records `prehistory_complete=false`.
Episodes close as `WINDOW_COMPLETE` (window ended) or `MAX_DURATION` (cut at
the limit). Neither means delivery recovered.

## Recording Triggers

| Trigger | Meaning in the record |
| --- | --- |
| Transition to `TCP_ACK_STALLED`, `SOCKET_UNAVAILABLE` or `COUNTER_REGRESSION` | ACK not advancing while output is pending, the target socket could not be observed (absent or not uniquely identified), or a counter was observed going backwards |
| Process or connection generation change | Reconnect or FFmpeg replacement |
| FIFO recovery start, FIFO counter change, new journal event | FFmpeg reported an output error or reopen |
| `notsent >= 256 KiB` for at least 3 s of source time | `QUEUE_RECORDING_CANDIDATE` |
| Collector or source gap, source time regression, unreadable input | The observation itself failed |

The 256 KiB and 3-second values are provisional **recording** conditions. They
are not fault thresholds and do not feed recovery. Normal `notsent` is near
zero (99th percentile about 60 KB on the earlier route), and interruptions
produced 0.5-2.1 MB, so the value sits between the two.

Three seconds are measured on source timestamps, not counted as three polls:
samples at 0, 1.1 and 2.2 seconds do not fire; a sample at 3.3 seconds does.
A repeated read of the same snapshot is not new evidence of persistence.

## Rates And Missing Data

- A rate is computed only between two distinct source samples on the same
  process and connection generation.
- A duplicate read, unreadable input, gap over 3.5 seconds, time regression,
  counter decrease or connection change stores `NULL`, never 0 Mbps.
- 0 Mbps is stored only when a newer source sample shows no ACK increase.
- Unreadable input is `SOURCE_UNAVAILABLE`, kept distinct from a WAN loss or a
  stopped broadcast. Source freshness and update-stall limits are 5 seconds.
- `fresh_observed_seconds` is observation time, not healthy delivery time or an
  SLI.

## Comparing With Independent Observations

Detail on the sender is useful only when placed next to evidence from elsewhere:
relay input, provider output and clock observations. The `report` command accepts
a file of independently observed intervals, each with the last good time,
the first bad time and a combined clock-uncertainty bound.

The lead time is reported as an interval:
`[last_good − trigger − uncertainty, first_bad − trigger + uncertainty]`.
If it contains zero, the result is `OVERLAPPING_INTERVALS`. A missing
uncertainty gives `UNKNOWN`.

Two signals derived from the same snapshot, such as the queue candidate and
`TCP_ACK_STALLED`, are not independent. Their difference is not validation.
The reference intervals typically have a resolution of several seconds, so
second-level lead times are reported as ranges, not points.

## Failure And Restart

| Situation | Behavior |
| --- | --- |
| Clean stop | `RECORDER_STOPPED` on the session |
| Crash, then restart | The open episode is closed as `RECORDER_RESTART_GAP`; committed rows remain |
| Storage budget or write failure | Exit code 75, reason written to status when possible; no retry, deletion, `VACUUM` or budget growth |
| Unknown existing database, symlink, second writer | Refused without modifying the existing file |

The memory window and uncommitted rows are lost if the process or host stops.

## Isolation

The [example unit](../../ops/systemd/stream-v3-transport-recorder.service) runs
the recorder as an unprivileged account with read-only access to the snapshot
and journal directory, and write access only to its state directory. The
journal is bind-mounted as a directory rather than a file, so atomic
replacement stays visible.

It also sets `PrivateNetwork`, a seccomp filter that denies network system calls, no
capabilities, `NoNewPrivileges`, inaccessible control sockets, 5% of one CPU,
64 MiB memory, 4 tasks, an 80 MiB file size limit and `Restart=no`. The service
account and the input read permission are provisioned locally.

## Running It

From the repository root, with a synthetic fixture and a new state directory:

```bash
python3 -B ops/scripts/stream_v3_transport_recorder.py collect \
  --source /absolute/path/to/fixture.json --state /absolute/path/to/new-state --steps 3
python3 -B ops/scripts/stream_v3_transport_recorder.py report --state /absolute/path/to/new-state
python3 -B ops/scripts/stream_v3_transport_recorder.py report --state /absolute/path/to/new-state --episode 1
python3 -B ops/scripts/stream_v3_transport_recorder.py report \
  --state /absolute/path/to/new-state --references /absolute/path/to/reference-intervals.json
```

`report` is read-only. It lists the latest 100 episodes, totals, trigger
counts and observed minutes; `--episode` prints one episode's timeline as JSONL.
Check `status.json` freshness and the process state as well as the database.

See the [recorder](../../ops/scripts/stream_v3_transport_recorder.py) and its
[tests](../../tests/test_transport_recorder.py).

## Operated Deployment

The recorder started on the Dell sender on 2026-10-29 at 09:59 JST. In the first
189 seconds it made 188 polls with 180 distinct source samples, 8 duplicates
and no gap. It saved three per-minute summaries; the first minutes had no
trigger, so no episode was written. At 10:05 JST the cgroup memory peak was
about 13.5 MB and the state used about 150 KB.

The broadcast Pod, FFmpeg process, connection generation and recovery
configuration were compared before and after deployment and were unchanged.
The first deployment check failed because it expected the status file within 2
seconds; under the CPU quota the first status took about 9.3 seconds. The
check now waits up to 45 seconds while still failing on process exit.

## Evaluation Window

The first two weeks check input stability, storage growth and how often
candidates fire, and whether the records classify events better than before.
The continuation criteria are in the
[retirement decision](route-baseline-retirement-decision.md#revisit-conditions).

## Limits

- Main FIFO sender only. Fallback delivery, relay and provider reception, public
  players and SLIs are outside the record.
- No clock input is configured yet, so cross-host lead times are `UNKNOWN`.
- No fault was injected in production. Event windows on a real fault,
  classification value, detection and precursor performance are not yet
  observed.
- Boot, power loss, multi-week operation and the storage limit are not yet
  observed in production.
- The 40 portable tests use real SQLite and synthetic sources; they do not
  establish production behavior.
