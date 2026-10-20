# Public Status Page Redesign: 2026-10-20

## Why it changed

After the route moved to the [self-healing fan-out](self-healing-oci-fanout.md),
the public page showed every delivery and relay field as not observed or
unknown. Two causes combined: the relay observer still read the stopped
signed-lease relay, and the monitoring host could no longer reach the relay
host after the home address changed. The observer now reads the MediaMTX relay,
and the monitoring host reaches the relay over the private tunnel. Live relay
values returned on the page at 11:33 JST.

The page also had a presentation problem. Incidents appeared as internal
identifiers, one card could list more than ten detail rows, and the reason a
budget needed action was not stated. The redesign answers three questions in
order: is the stream live now, what happened in the last 24 hours, and how
reliable has it been against its targets.

## Structure

| Section | Content |
| --- | --- |
| Status banner | Current delivery state only. Warning-level reliability alerts stay in the reliability section, so a budget warning never looks like an outage. |
| Now | YouTube, Twitch, and the upload path, with a route diagram. While the direct YouTube fallback runs, the page says so and shows both uploads and their total rate. |
| Last 24 hours | Twitch LIVE state, upload bitrate p95, and encoder restarts on one shared time axis. |
| Reliability | One table row per target; a row opens its breakdown and the metric behind it. |
| Incidents and recovery | Named incidents and their recovery, newest first. A YouTube interruption in the last hour that has already recovered is a warning, not an outage. |
| System checks | Collector and freshness checks, collapsed by default. |

Each section renders its own unknown state when its input is missing or stale,
so one failed collector does not blank the page. Status colors are fixed (good,
warning, serious, critical) and always paired with a label or icon. The page
supports light and dark themes and was checked at 400 pixels wide.

## Implementation and publication

The page uses only [`ops-20261020.css`](../../ops/public-publisher/site/public/assets/ops-20261020.css)
and [`ops-20261020.js`](../../ops/public-publisher/site/public/assets/ops-20261020.js).
Both were added to the uploader's fixed allowlist. The page reads six public
JSON artifacts that were already in that allowlist. Earlier versioned assets remain in the
repository and in the allowlist.

The redesigned page was published from the Raspberry Pi at 11:49 JST on
2026-10-20. The previous page stayed available on the live site as a separate
file during the transition; it is not part of this public source.

## Limits

Checks were visual, in a browser, in light and dark themes and at a narrow
width. This port adds no automated browser fixture for the new page. The
fallback upload display has not yet been seen during a real fallback.
