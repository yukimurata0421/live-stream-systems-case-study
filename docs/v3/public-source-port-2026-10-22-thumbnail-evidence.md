# Public Source Port: Thumbnail OCR And Bounded Evidence

This follow-up updates the [thumbnail contract](twitch-thumbnail-monitor.md)
after the [rollover review](public-source-port-2026-10-22-thumbnail-review.md).
[Current status](../current-status.md) owns later observations and gaps;
earlier dated records remain intact.

## Source And Scope

The source is commit `0b7bba914d6a`. Only selected source, tests, configuration
examples and documentation are ported; public host aliases and English prose
follow existing conventions. Source selection, digests and checks are in
[the port record](../../publication/thumbnail-evidence-port-20261022.json).

Four unrelated in-progress Twitch SLI and publisher files, other working
changes and their index entries are preserved; shared indexes and the source
map include only this port's entries. There is no provider acquisition,
production deployment, restart, notification or push in this port. Delivery
routes, backup, API collection and recovery authority are outside the change.

## Cause

From 10:29 to 15:59 JST on 2026-10-22, the operated history recorded 64
uncertain cycles across eleven images, not 64 independent failed images. Five
distinct images had retained ROIs. Small compressed UTC characters produced
low-confidence values or malformed strings. This was observation-quality loss,
not evidence that public video stopped.

## Changes

- **Interpolation retry.** When ordinary, contrast and conservative binary reads
  all fail, one paired retry uses interpolation. It needs at least one confident
  original reading, keeps every parseable original value, and requires both
  final confidences and the nine-hour offset. Conflicting original pairs stay
  rejected. There is no voting, predicted time or confidence reduction. The
  cycle limit remains 15 seconds and eight OCR commands.
- **Bounded evidence.** Each distinct abnormal image/stream/ROI combination may
  retain a small ROI archive during an episode, deduplicated across restart.
  Limits are 8 MiB of allocated space, seven days and 64 archives. One current
  archive is protected; a full store skips new evidence rather than exceeding
  the cap. Only named JST/UTC regions of at most 320x96 pixels qualify. Normal
  JPEGs stay in memory and are discarded. No new database, full-frame archive or
  shared evidence-store change is added.
- **UTC ROI.** The UTC region gains three pixels on its left at 1080p, based on
  the comparison below. The example configuration and default agree; explicit
  configuration still takes precedence. Cache identity includes ROIs.
- **Diagnostics.** Method, rejection reason, stage outcome, retry-start and
  final-acceptance counters separate work performed from a verified clock.
  Cached cycles do not inflate counts; new counters do not backfill history.

## Operated Rollout Evidence

The monitoring host activated interpolation at 16:30 JST and the evidence, ROI
and diagnostic extension at 17:09:42 JST, before this port. These are
operated-system observations, not deployment of this public checkout.

The UTC ROI was compared on three images taken at least six minutes apart,
using the ordinary read only:

| Capture (JST) | UTC value | Confidence, previous ROI | Confidence, widened ROI |
| --- | --- | --- | --- |
| 16:48:12 | 07:46:45 | 94.98 | 96.08 |
| 16:54:17 | 07:52:45 | 37.00 | 96.80 |
| 17:00:33 | 07:58:45 | 38.75 | 96.70 |

The UTC value and nine-hour pair were unchanged. Ordinary-read acceptance rose
from one of three to three of three. This is not the full retry pipeline's
success rate or evidence for all times and compression conditions.

Saved images were replayed in isolation (0.2-core CPU, 256 MiB memory, no swap,
no network):

- All five available paired clocks were verified, compared with one using the
  earlier reader. Ten mismatched or missing-anchor cases remained rejected. The
  six unretained image types could not be reanalyzed, and historical uncertain
  observations were not rewritten as successful.
- Six incorrect or missing-clock cases for the widened ROI remained rejected.
- The five saved-image cases plus their negatives took 49.592 seconds in total
  with about 35.1 MiB peak memory, which is distinct from steady-state load.

After activation, at 17:12 JST a new image passed the original reader with
paired confidence about 95.8/96.7, completing capture and analysis in 1.546
seconds. The next cached cycle added no OCR. In that short window worker memory
peaked near 52.9 MiB, old evidence occupied 24 KiB and no normal image files
were present. New-format capacity behavior was tested in isolation, without a
production fault.

## Validation And Limits

Credential-free regressions, documentation/catalog links, public language,
source-map digests and selected secret checks cover the committed candidate.
The development candidate passed 118 related tests; the public candidate passed
142 tests and 756 subtests, including documentation and API compatibility. A
broader legacy-probe check had the same six failures on both the candidate and
the unchanged development commit: five require helpers absent from committed
source, and one expects unsupported legacy v2 exporter input. Those
pre-existing gaps are outside this port and are not reported as successful.

Not established by this port:

- Twenty-four-hour OCR quality, daily renewal under this version, and ADS-B
  noninterference.
- Notification delivery. An earlier investigation found successful HTTP
  responses for six warning/recovery pairs on the existing route, but message
  persistence, display and human receipt were not established. This port sends
  no notifications and changes no notification transport or policy.
