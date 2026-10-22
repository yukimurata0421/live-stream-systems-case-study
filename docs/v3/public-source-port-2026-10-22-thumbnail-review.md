# Public Source Port: Thumbnail Rollover And Missing Observations

This follow-up updates the [thumbnail contract](twitch-thumbnail-monitor.md)
after the [initial port](public-source-port-2026-10-22-thumbnail.md). The initial
record remains a dated account of its selected source. [Current status](../current-status.md)
owns later observations and unobserved behavior.

## Source And Scope

The source review is commit `c27f0ee0af1f`, on top of `740bcc00` and `9eb59246`.
The public base is `ea3ee8c6`. Code is adapted to the existing public host aliases
and English prose; machine identifiers and observation contracts are retained.
The related development checkout also receives the dependency modules required
to inspect and test the standalone worker. Source selection is recorded in
[`publication/thumbnail-review-port-20261022.json`](../../publication/thumbnail-review-port-20261022.json).

Four unrelated in-progress Twitch SLI and publisher files are outside this
change. Their working copies and source-map entries are preserved. Validation
uses the selected commit candidate with their committed versions, rather than
including those independent edits. There is no push, deployment, service restart,
API request, provider video acquisition or test notification in this port.

## Review Changes

- Clear a channel thumbnail's ETag when the API stream ID changes, including
  when the RAM sidecar is temporarily unavailable. Clear an unusable cache after
  analysis exceptions as well.
- Keep a pre-stream image in `WAITING_NEW_STREAM_THUMBNAIL`; 304 responses may
  continue that wait without creating fault evidence. Later regressions after
  a valid stream clock still remain uncertain.
- Give the first valid stream clock at most twelve minutes from `started_at`.
  Persist the origin across restart and API UNKNOWN. End the allowance after a
  valid clock; it does not apply to worker publication loss.
- Change the stale-image warning to 840 seconds. This allows a missed six-minute
  provider refresh and poll phase. It is provisional, not a validated SLO.
- Retain publication age when state is missing or invalid, and warn if the
  metric itself disappears for ten minutes. Poll spacing and Retry-After are
  explicit waiting states, separate from failed capture.
- Use the unchanged repository unit with an atomic `current` release symlink,
  matching Python, library and OCR configuration paths.

## Operated Evidence And OCR Follow-Up

On the operated monitoring host, low-confidence UTC reads caused an observation
quality warning from about 11:06 JST on 2026-10-22. This was not a stream-rollover
event or proof of failed public video playback.

Reanalysis of already retained abnormal ROIs read JST 11:05:15 and UTC 02:05:15.
The original UTC confidence was about 57.9; binary threshold 128 retained both
values with confidences about 92.9 and 94.1. Threshold 96 produced misreads.
The added single binary pass therefore requires both original clock values to
match exactly and both confidences to meet the unchanged minimum of 70.
Different values or remaining low confidence stay uncertain. This small case
does not establish a general OCR error rate.

At 11:22 JST, operated capture and OCR were successful and the three thumbnail
warnings were inactive. That live image succeeded with the ordinary read; the
binary retry is not established as the cause of live recovery. The preceding
evidence is operated-system evidence, not deployment of the public example.

## Validation And Limits

The source review passed 141 tests with two environment-dependent OCR skips.
Updated related source tests, documentation checks, the source map, English text
and Prometheus rule fixtures are verified for this public candidate; their exact
results are recorded in the publication record. Tests use temporary state and
mocked transport. Real-image reanalysis above used retained abnormal ROIs only.

Daily rollover at 2026-10-23 00:00 JST, twenty-four-hour quality, and actual new
warning delivery remain unverified here. JST-midnight replay, bounded startup
allowance and warning tests do not establish a real provider rollover. No
Critical, SLO denominator or recovery authority is added. Normal images remain
RAM-only and the worker's no-swap setting is retained.
