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

## Follow-up the same afternoon

Two alerts stayed active after the redesign.

- The external uptime check matches the text "Operational reliability" on the
  page. The redesign had dropped it, so every checker region failed while the
  page itself served normally. The heading was restored at 13:41 JST and a test
  now guards the text.
- An audio budget alert came from three short audio-check failures, each during
  planned work that day: the plan B switch, the relay drill, and the RTMPS
  change that restarted the main Pod three times. The alert logic was correct.
  Budget alerts are evaluated hourly over the past one to six hours, so the
  page now says they can stay listed for up to an hour after the cause clears.

The same review changed two judgements on the page. Twitch renewal now follows
the relay keeper's own state instead of a fixed 00:00 to 00:15 JST window, since
a renewal can also start at the 46-hour limit and can take about 13 minutes.
After the relay recovers, the five minutes in which the fallback still runs are
shown as a warning ("fallback ending") rather than as the relay being down. Both
were checked by rendering the page headlessly with fixtures for the current
state, a recovering fallback, and a renewal in progress.

## Reliability rows that did not appear to move

"Upload within ceiling" stayed at 88.91% for hours. Its 1,118 minutes above the
5.0 Mbps ceiling all came from the earlier route, more than 24 hours old but
still inside the 7-day window, while every minute of the last 24 hours was
within the ceiling. A rolling 7-day ratio only changes as old minutes leave the
window, so the value was correct but hid the recovery. Twitch LIVE availability
was 15.8% over seven days because Twitch was not relayed for most of that week;
on a bar fixed to 90-100% it sat at the left edge.

Each row now shows its 24-hour value beneath the 7-day value and as a ring on
the bar, and a bar starts below 90% (in 10-point steps) when a value is lower.
The daily renewal row lists ok, failed, and unknown renewals. The collector
publishes the 24-hour value as `recent_24h`; it stays null when unavailable.

## Making the 24-hour ring readable (2026-10-21)

When the 24-hour value was below the 7-day value, its ring sat inside the blue
bar and looked like a small white dot. The ring now has a dark outline above
the bar and a tooltip, the column header names bar, ring, and target line, and
the status adds the 24-hour gap when the last day is further below target than
the week. A bar now starts one 10-point step below its lowest value, after a
Twitch value of 20.2% on a 20-100% scale left its bar empty. A renewal row with
no completed result shows "no result yet" rather than "below target".

## Limits

Checks were visual, in a browser, in light and dark themes and at a narrow
width. This port adds no automated browser fixture for the new page. The
fallback upload display has not yet been seen during a real fallback.
