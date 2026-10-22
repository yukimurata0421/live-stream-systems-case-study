# Public Source Port: Thumbnail OCR And Bounded Evidence

This follow-up updates the [thumbnail contract](twitch-thumbnail-monitor.md).
[Current status](../current-status.md) owns changing observations and gaps;
earlier dated records remain intact. Detailed source selection, digests and
checks are in [the port record](../../publication/thumbnail-evidence-port-20261022.json).

## Cause And Changes

The operated history from 10:29 to 15:59 JST on 2026-10-22 contained 64 uncertain
cycles across eleven images, rather than 64 independent failed images. Five
distinct images had retained ROIs. Small compressed UTC characters produced
low-confidence values or malformed strings. This was observation-quality loss,
not evidence that public video stopped.

After ordinary, contrast and conservative binary reads fail, interpolation may
retry the pair. It preserves every parseable original value, needs one original
confident anchor and requires both final confidences and the nine-hour offset.
Conflicting original pairs remain rejected. There is no voting, predicted time
or confidence reduction. The limit remains 15 seconds and eight OCR commands.

Evidence now retains distinct abnormal image/stream/ROI samples through an
episode while deduplicating across restart. Limits are 8 MiB of allocated space,
seven days and 64 archives. One current archive is protected; a full store can
skip new evidence without exceeding the cap. Only named JST/UTC regions of at
most 320x96 pixels are eligible. Normal JPEGs stay in memory and are discarded.
No new database, full-frame archive or shared evidence-store change is added.

The UTC ROI adds three pixels on its left at 1080p. The example configuration
and default agree; explicit configurations still take precedence. Cache identity
includes ROIs. Method, rejection reason, stage outcome, retry-start and final
acceptance accounting distinguish work performed from a verified clock. Cached
cycles do not inflate counts; newly added counters do not backfill history.

## Earlier Operated Evidence

Before this port, the monitoring host separately activated interpolation at
16:30 JST and the evidence/ROI/diagnostic extension at 17:09:42 JST. These are
operated-system observations, not deployment of this public checkout.

- Saved-image replay verified all five available paired clocks, compared with
  one using the earlier reader; ten mismatched or missing-anchor cases remained
  rejected. The six unretained image types could not be reanalyzed. Historical
  uncertain observations were not rewritten as successful.
- Three later image comparisons, at least six minutes apart, held the UTC value
  and nine-hour pair unchanged. Ordinary-read UTC confidence changed from
  94.98/37.00/38.75 to 96.08/96.80/96.70. Raw-read acceptance changed from one of
  three to three of three; this is not the full retry pipeline's success rate
  or evidence for all times and compression conditions.
- Six incorrect or missing-clock cases for the widened ROI remained rejected.
  The replay used 0.2-core CPU, 256 MiB memory, no swap and no network. The five
  saved-image cases plus their negatives took 49.592 seconds in total, with
  about 35.1 MiB peak memory; this is distinct from ordinary steady-state load.
- At 17:12 JST a new image passed the original reader with paired confidence
  about 95.8/96.7, completing capture and analysis in 1.546 seconds. The next
  cached cycle did not add OCR. In that short window worker memory peaked near
  52.9 MiB, old evidence occupied 24 KiB and no normal image files were present.
  New-format capacity behavior was tested in isolation, without a production
  fault. No twenty-four-hour quality or ADS-B noninterference claim follows.

The earlier warning investigation found successful HTTP responses for six
warning/recovery pairs on the existing notification route. Message persistence,
display and human receipt were not independently established. This port sends
no notifications and changes no notification transport or policy.

## Source Transfer And Verification Boundary

The canonical source is commit `0b7bba914d6a`.
Only selected source, tests, configuration examples and documentation are
ported. Public host aliases and English prose follow existing conventions.
The four independent Twitch SLI/publisher paths, unrelated working changes and
their index entries are preserved. Shared indexes and the source map commit
only this task's entries, leaving other work uncommitted.

Credential-free regressions, documentation/catalog links, public language,
source-map digests and selected secret checks cover the candidate committed
snapshot. Earlier real-image evidence is reported separately above. No new
provider acquisition, production deployment, restart or push occurs in this
port. Delivery routes, backup, API collection and recovery authority stay outside
the change. Daily renewal and long-term OCR quality remain open.

The development candidate passed 118 related tests; the public candidate passed
142 tests and 756 subtests, including documentation and API compatibility.
Source-map and language checks passed. A broader legacy-probe check had the
same six failures on both the candidate and the unchanged development commit:
five require helpers absent from committed source, and one expects unsupported
legacy v2 exporter input. Those pre-existing gaps were not included in this
OCR port or reported as successful.
