# Relay Return-Feed Clock Monitor

A relay can keep accepting video while a platform forward has failed, and a
clock's pixels can change without proving that its displayed time advanced.
This monitor records those facts separately. It reads the OCI common relay and
the direct YouTube fallback sender, checks the overlay clock, and combines the
result with existing native relay observations. It does not restart anything.

This page is the stable contract. [Observability](../observability.md#video-observation-layers)
defines how far each observation layer reaches, and the
[architecture](../architecture.md#clock-observation-path) owns the host and data
paths. [Current status](../current-status.md) owns recent changes and unobserved
behavior. The [clock observation case study](clock-observation-case-study.md)
explains why the design changed; dated measurements are in the
[source-port record](public-source-port-2026-10-22.md) and the
[evidence and UTC fallback record](public-source-port-2026-10-23-relay-clock.md).

## Scope Labels

Each metric carries one fixed scope label for the layer it belongs to.

| Scope label | Layer |
| --- | --- |
| `dell_oci_relay_dell` | Return-feed clock from the OCI common relay. |
| `dell_backup_sender_dell` | Fallback-sender clock from the direct YouTube backup encoder. |
| `oci_forward_sender` | Native per-destination forward state and byte progress. |

`oci_relay` and `youtube_fallback` are independent targets. An unavailable OCI
reader cannot substitute for the fallback sample. A fallback with no requested
producer is standby. This worker does not acquire the public player.

## Bounded Capture

[FrameReceiver](../../src/stream_v3/frame_receiver.py) keeps one FFmpeg process
per active source, continuously drains it, and retains only the latest bounded
frame slot. It timestamps a complete decoded frame when the pipe receives it,
before image conversion or OCR. PTS progression, bursts, wall-clock changes,
decoder warnings, and local packet drops become timing-quality diagnostics.
They cannot isolate every upstream queue from a constant decoder delay.

The worker samples every ten seconds. FFmpeg is single-threaded and has input
and inactivity timeouts; its process group is killed and reaped on closure.
OCR has a separate finite deadline. Resource or acquisition failures remain
`UNKNOWN`. Ordinary full-frame JPEGs stay in memory-backed storage, with two
recent images per target. Abnormal evidence has capacity and retention limits.

Initial stream probing allows five seconds and 5 MB, with 0.1-2 seconds of
reconnect jitter so a reconnect does not keep landing on the same GOP phase. A
one-second, 1 MiB probe left the reader unable to start for hours with FFmpeg
5.1.9; the [case study](clock-observation-case-study.md#the-reader-went-blind)
describes that failure. Missing codec parameters have a separate
`capture_probe_failed` reason. Cumulative PTS versus monotonic drift above one
second invalidates delay without claiming a stream fault.

The fallback encoder keeps one primary synchronous output. Its secondary UDP
output uses a 32-packet FIFO with drop-on-overflow and ignore-on-failure. The
receiver keeps at most 4 MiB / six seconds and a 256-packet pipe-input queue.
Stopping the observer must not wait on the primary output. The module adds no
second encode and leaves the existing fallback controller in charge of replicas.

## Clock And Missing-Data Semantics

[VerifiedClockAnalyzer](../../src/stream_v3/verified_clock.py) reads both JST and
UTC on every sample. Each reading must pass the confidence threshold and both
must agree modulo the nine-hour offset. A failed pair gets one alternate
contrast pass; repeated image transforms are not independent votes. The previous
accepted reading is cached rather than OCRed again.

For the relay target only, a pair rejected by both passes gets one more UTC
read from a region three pixels wider on the left. It runs only when the
original JST reading is confident, fixes that JST, and requires the new UTC to
pass the same confidence and nine-hour rules without changing any UTC value that
was already parseable. Time conflicts, an unreliable JST, process failures and
timeouts skip it with a fixed reason. A reading accepted on the current region is
never re-read, so the wider region cannot replace a working one. A relay sample
makes at most five OCR calls; a fallback sample retains at most four. Both
targets share the unchanged eight-second cycle deadline, with at most one
second per OCR call. `ocr_roi_method` records which region was used.

| Result | Meaning |
| --- | --- |
| `WARMUP` | No comparable accepted reading after startup or a source-identity change. A reader reconnect alone preserves the clock history. |
| `ADVANCING_CONFIRMED` | Positive clock progress agrees with monotonic elapsed time within five seconds. |
| `STALLED_SUSPECT` | Accepted paired readings contain the same clock value. |
| `STALLED_CONFIRMED` | At least two equal-value comparisons and 60 seconds of observed freeze. |
| `OCR_UNCERTAIN` | Pair/confidence failure, cadence drift, regression, or a jump. |
| `BLACK_FRAME` | The frame meets the configured black-screen test. |
| `UNKNOWN` | Capture, timing source, analysis resources, or publication is unavailable. |

Five to 30 seconds of cadence error is recorded as drift; larger errors and
regressions remain uncertain. A single uncertain sample is not a paging event.
Image differences remain diagnostic; `ADVANCING_PROBABLE` cannot set the new
`clock_advancing` metric to one.

A failed capture preserves the previous reading and freeze history. The worker
publishes the equal-value span, observed freeze time, and a gap flag separately.
A missing interval does not add observed freeze time and cannot confirm a
freeze by itself. A paired reading also does not prove aircraft-data or audio
correctness.

## Delay And Native Evidence

The UTC reading is mapped to the nearest day and subtracted from decoded-frame
arrival time. `stream_v3_monitor_probe_stream_delay_seconds` is display age,
including rendering, encoding, upstream buffering and decoding; it is not a
network-only round-trip measurement. The clock's one-second quantization remains.
Invalid timing or uncertain progression removes the valid delay sample.

Gauges support trends and steps; a classic histogram records only unique valid
samples. A second scrape does not increment its count. Absolute thresholds need
a measured baseline for each scope. The dated review discusses an initial
warning proposal; source publication does not enable it.

[Native facts reduction](../../src/stream_v3/monitor_facts.py) uses the existing
relay collector, not an extra OCI poller. The native source has a 60-second
cadence and a 90-second freshness limit. Counter comparisons require the same
boot, connection/provider identity, and increasing source time. Resets warm up
the comparison, and the scheduled Twitch pause is distinguished from failure.

A fresh relay-not-running or publisher-not-ready fact, a black frame, or an
OCR-confirmed freeze can support `target_bad`. A TLS failure, timeout or decode
failure alone is `observation_unknown`. Publisher absence does not identify the
physical cause of a missing input. Destination facts and existing platform/API
observations retain their separate scopes and owners.

## State, Trust And Metrics

The worker publishes `stream_v3.oci_monitor_probe.v2` inside the existing signed
`stream_v3.oci_monitor_bundle.v1`. Required v2 capabilities are continuous
capture, OCR every sample, and paired-clock verification. The old synthetic
viewer file and metrics are not overwritten.

The importer verifies the SSH host key, current boot, HMAC, source CA identity,
TTL, image hashes, Deployment identity, and the current desired versus running
immutable observer image. The sender's immutable tap and current producer are
also bound. Whole-template and worker/image baseline changes are diagnostic;
they do not reject otherwise compatible observations. A known v1 revision is
readable during migration but cannot obtain v2 health semantics.

Boot, Pod and cycle identify each sample. Bounded long polling removes fixed
phase delay; duplicate cycles do not enter sample history. Collector failures
have a separate history. The exporter refreshes these small facts on every
scrape while retaining the expensive legacy collector cache.

Existing OCI/fallback metric names remain. New `stream_v3_monitor_probe_*`
series expose scope, classification, bounded reason/stage, native freshness,
forward flow, freeze coverage, cadence error, delay quality and histogram,
configuration change, and duplicate counters. `clock_advancing=1` requires a
fresh capture, compatible runtime/source binding, and paired OCR progression.
These are observation inputs. The independent monitoring-host integration adds
`ViewerClockObservationUnavailable` after a required clock gap exceeds five
minutes, and `ViewerClockObserverUnavailable` after observer availability or
quality metrics are missing for five minutes. Fresh, bound fallback standby is
excluded. It reuses the existing warning delivery loop; two distinct fresh
observations are required to resolve a warning. No recovery path is added.

The monitor's ten-second history rotates after about an hour. A small recorder
([script](../../ops/scripts/relay_output_gap_recorder.py),
[user timer](../../ops/systemd/stream-v3-relay-output-gaps.timer)) appends each span
where frame capture itself failed between two verified clock readings, so daily
interruptions remain comparable. A clock jump alone is not counted: after an
interruption the live clock simply shows the current time, and a single OCR
misread leaves the same spacing. The span estimates unseen relay output, not
viewer playback; the reader shares the delivery host's WAN.

Low UTC confidence on returned frames was the main source of unknown
picture-freshness minutes while frames and forward bytes continued. The
fallback read above addresses it without a binary-threshold retry;
[current status](../current-status.md) tracks whether it has rescued a
production sample yet. The
[Twitch thumbnail contract](twitch-thumbnail-monitor.md) adds a slower
provider-image layer with separate age and refresh semantics.

## Clock Evidence

Each sample records `ocr_pair_stages`: the strings, confidences, bounded error
codes and pair acceptance of the `original`, `contrast` and, when attempted,
`expanded_utc` reads. A rejected or retried read can therefore be explained
without guessing which stage failed.

[ClockEvidenceStore](../../src/stream_v3/verified_clock_evidence.py) keeps small
native-resolution archives (`stream_v3.clock_pair_evidence.v1`):

| Kind | When | Regions |
| --- | --- | --- |
| Abnormal | Uncertain, stalled or black samples | Previous and current JST, plus `jst.pgm`, `utc.pgm` and `utc_context.pgm` |
| `roi_retry` | A sample accepted only by the wider UTC read | `jst.pgm`, `utc.pgm` (current-region pixels) and `utc_context.pgm`; `expanded_utc_bounds` gives the region read |
| `periodic_roi` | An advancing relay sample, at most once every 60 seconds | The same three regions, as normal controls |

`utc_context.pgm` extends the UTC region by eight pixels left and right and four
above and below, so wider crops can be replayed from native pixels rather than a
resized JPEG. Metadata binds the sample, capture time, display-JPEG hash, source
resolution, ROI settings and pixel bounds. Regions are at most 320x96 pixels.

Archives travel in the signed bundle. The importer checks sample binding,
member names and count, PGM format, and 256 KiB per archive. Each host keeps at
most 8 MiB of allocated space for 24 hours and skips new evidence when full;
storage failures are recorded and never fail the observation. The periodic
cadence is not an SLI denominator, and ordinary full frames are still kept only
in memory.

## Configuration And Local Checks

The [worker](../../ops/config/oci_monitor_probe.example.json) and
[importer](../../ops/config/oci_monitor_observer.example.json) examples contain
placeholders. Calibrate both clock ROIs at the input resolution. Configure the
current Deployment and immutable sender identities, CA and separate HMAC key.
Never put those private values into this repository.

The standalone [Deployment](../../deploy/k3s/oci-monitor-probe/deployment.yaml)
is not included in the ordinary shadow overlay. It requires an operator-built
immutable image, configured private Secret/ConfigMap, and an actual allowlisted
relay address in the NetworkPolicy. It retains one replica, non-root execution,
read-only root filesystem, no API token, no GPU/RBAC, 500m CPU / 512 MiB memory
limits, and bounded memory volumes. It is not ready to apply unchanged.

The importer example uses a dedicated `stream-monitor` account. Its fixed root
pre-step can read the existing native collector's protected raw file and writes
only an allowlisted projection into observer RAM. The native collector and its
machine binding must already be configured for the generic `relay-host`
identity. It grants no remote mutation or general executor rights.

Credential-free tests use temporary state, mocked transport and a finite local
FFmpeg child if available:

```bash
python3 -m pytest -q tests/test_verified_clock.py tests/test_verified_clock_evidence.py \
  tests/test_frame_receiver.py tests/test_monitor_facts.py tests/test_oci_monitor_probe.py \
  tests/test_clock_exporter_cache.py
```

The separate isolation fixture expects a test-owned MediaMTX binary, the backup
argument builder, a font, and matching FFmpeg/Tesseract inside an isolated
container. It must have no production credentials or network path. That fixture
is not run against production by the public test suite.
