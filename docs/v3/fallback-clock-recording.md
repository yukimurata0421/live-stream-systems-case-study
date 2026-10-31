# Fallback Clock Skew and Bounded Recording

Dated record, October 30-31, 2026. This page covers steps 1 and 2 of the
[revision history](fallback-policy-revisions.md). The current start and stop
rule is the [fallback decision contract](fallback-decision-contract.md).

A small clock difference between the sender and relay caused an unnecessary
direct fallback on October 30, 2026. The controller's future-timestamp check
accepted no negative age: a source timestamp three milliseconds ahead of the
sender was rejected as `source_future`. The controller lost its normal baseline
and started the backup after the existing failure window. Repeated rejections
also reset the continuous-health window needed to stop it.

On October 30 the immediate fix allowed one second of future skew while
preserving the 20-second freshness limit and the 30-second start / 300-second
stop conditions. That was provisional: the largest logged future rejection was
65 ms, and one second matched the earlier public-snapshot repair. It was not a
proven bound on clock error.

On October 31 the bound route-facts path replaced that fixed tolerance with a
nonce-bound clock interval. The oldest possible source/copy/progress age must
be at most 20 seconds; an interval wholly more than 5 ms in the future is
rejected. That revision required a clock model for route-facts validation;
there was no model-free one-second branch. Host/boot/destination and media
progress checks remain. Later steps separated clock faults from media failure;
see the [decision contract](fallback-decision-contract.md). See the
[current clock contract](cross-host-observation-time.md), including its one-way
compatibility and rollout order.

The [recorder](../../ops/scripts/stream_v3_fallback_clock_recorder.py) reads
controller status to evaluate clock-error and uncertainty budgets. It neither
tunes a threshold nor dispatches recovery or an extra route-health request.

## What the recorder measures

The recorder checks the controller's atomic status file every second and writes
each distinct controller observation. The controller waits 15 seconds between
polls; HTTP and Kubernetes reads add time to that interval. This is a record of
the controller's actual observations, not a claim of exact 15-second scheduling.

Records contain source/copy/progress age, HTTP start/completion timestamps and
monotonic elapsed time, health/reason, backup replicas, controller PID and reset
counters. They retain negative age rather than clamping it to zero. Source
Records also contain assessment, cause ID and the secondary-recovery veto.
Source generation within the HTTP request gives lower and upper bounds for
`remote clock - local clock`. These bounds include asymmetric transport delay
and server work, and assume the local wall clock did not step within the request.
The wall-versus-monotonic duration difference is recorded for that check. HTTP
elapsed time is not a pure network RTT or an NTP offset.

The recorder also stores the verified model's `clock_offset_seconds`,
`clock_uncertainty_seconds` and `clock_network_rtt_seconds`, plus
`source_age_upper_seconds`, `copy_age_upper_seconds` and
`progress_age_upper_seconds`. These are corrected age upper bounds; the full
lower/upper age pairs remain in the controller status. They are distinct from
the raw signed ages and the older HTTP-bracket bounds. Older records or an
unavailable model leave these fields null; they are not retroactively repaired.

A bounded background worker samples both hosts about once a minute using the
same read-only Python probe. It records chrony or systemd-timesyncd tracking,
synchronization state, daemon identity and selected events. Journal reads cover
the current system journal and its immediately preceding archive, selecting the
last two minutes of the time daemon's records. Arbitrary messages, addresses and
NTP server names are omitted. A failed component read remains explicit; it does
not mean that no correction occurred. Tracking values can still be retained
when the journal component times out.

## Collection and storage contract

The public [job example](../../configs/fallback-clock-recording.example.json),
[SSH example](../../configs/fallback-clock-observer.ssh_config.example) and
[service template](../../ops/systemd/stream-v3-fallback-clock-recorder.service.in)
describe the collection inputs. They contain expired dates and non-working
bindings. They are source artifacts, not an installation or authorization for a
production collection.

A configured job fixes both start and deadline, expected sender/relay machine
hashes, an existing HDD mount and filesystem UUID, an output directory, a byte
limit and a free-space reserve. Hashes use the exact bytes of `/etc/machine-id`,
including any newline. Strict SSH host-key checks and a read-only observer
account remain required. Mount/UUID checks do not prove that an arbitrary mount
is a physical HDD; the operated investigation verified its actual device.

Each process creates a new segment and flushes/fsyncs every record. Restarting
keeps the same job, deadline, earlier segments and cumulative byte budget. The
one-day example uses 32 MiB and a 1 GiB free-space reserve. A capacity, storage
or deadline limit stops collection rather than deleting evidence or falling
back to another filesystem. Longer retention requires a separately configured
bounded HDD job; it does not extend this collection automatically.

The template bounds CPU, memory and tasks and exposes only configured SSH
files and the output directory. This recorder has no recovery authority.

## Summary and review

`summary.json` is replaced atomically about once a minute and at stop. It reports
sample counts, age/HTTP/clock distributions, future-skew observation spans,
health/reason counts, PID-scoped reset deltas, gaps over 45 seconds, incomplete
records, clock events and component-read errors. A `DEADLINE_REACHED` result means
the configured window ended; inspect coverage and gaps separately.

Keep raw observations and summaries private. Compare ordinary operation with
time synchronization, daemon restarts and missing-data windows before choosing
an error allowance. A one-day maximum does not bound rare future excursions.
No summary automatically tunes the controller.

## Evidence scope

The operated investigation confirmed fallback from 16:07:28 to 18:31:59 JST,
with ten resets of the healthy window. Separate minute-level observations showed
roughly 4.2 Mbps of relay input and provider output, but one YouTube-output
observation was missing/non-flowing. Short interruptions and provider ingest
selection were not established by those counters.

A private one-day collection started at 21:12 JST on October 30 with an HDD
destination and an October 31 21:12 JST deadline. The initial checkpoint had
controller samples and clock records from both hosts, with the media workloads
and transport identity unchanged. The full window, final coverage and resulting
tolerance review remain unobserved at this source-port checkpoint.

The public tests exercise projection, clock parsing, finite deadlines, restart
deduplication, storage guards, capacity stops and summaries using local fixtures
and mocked command output. They do not contact production or qualify the public
example on a real relay. See the [port record](../../publication/fallback-clock-port-20261030.json)
for file hashes and local validation.
