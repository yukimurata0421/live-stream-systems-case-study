# Delivery Diagram and Trend Display

The public diagram binds SVG arrows and the mobile route to the same published
link observations. Dell-to-OCI input, OCI-to-YouTube output, and OCI-to-Twitch
output retain independent counter rates. A Twitch-only disconnect or planned
renewal does not select the direct YouTube route.

The direct Dell-to-YouTube path bypasses OCI. When fresh relay observations show
input stalled/disconnected or the relay stopped, relay YouTube output is not
flowing, and a contemporaneous public snapshot reports connected YouTube ingest,
a healthy watchdog, and the same live URL, the diagram highlights **inferred
direct delivery** in amber. The two snapshots must be within 90 seconds; snapshot
and relay-source age must each be at most 180 seconds. Missing, future, stale,
failed, or contradictory observations do not establish a direct route.

The existing public projection does not contain the runtime's selected route or
a direct-path byte counter. Consequently the display does not certify a route
switch, and direct bitrate remains **Not measured**. Collector freshness is not
an independent receiver verification. This display neither authorizes nor
executes failover, recovery, or provider reconnects.

Trend charts position observations by their timestamps over the declared
window. Null and nonfinite values remain missing; missing intervals break the
line instead of collapsing time or joining both sides. Conflicting duplicate
timestamps are treated as missing. Numeric zero remains an observation, and
missing summary values display `n/a`. Axis labels use JST.

## Verification

The candidate passed 25 browser assertions covering route inference, stale and
missing evidence, per-provider isolation, timestamp spacing, gaps, and numeric
zero. Desktop and 390-pixel mobile fixtures rendered the relay and inferred
direct paths; mobile had no horizontal overflow. The standalone fixture contains
22 core assertions and can be opened from
`ops/public-publisher/site/tests/graphs-20261014.html` in a browser.

These are display fixtures, with no production route switch or fault injection.
The public-site deployment remains held after a host-contract update; source
changes and browser checks do not establish live publication. Resume publication
in a new task after reacquiring the publisher identity, current file hashes,
rollback backup, and fresh source evidence. Upload versioned assets and confirm
origin availability before switching the HTML entry point.
