# Executive Summary

`stream_v3` is a continuously operated aircraft-map broadcast built on a home
network. It combines live aircraft data, a custom map, weather, and music, then
sends video through an OCI cloud relay to YouTube and Twitch. The important
engineering work is keeping the service recoverable while the network and
individual software components fail.

## What Was Built

| Part | Purpose |
| --- | --- |
| Aircraft-data source and map | Turn receiver data into a readable live view. |
| Dell media runtime | Render, mix audio, encode once, and feed one network sender. |
| OCI relay | Receive that input and forward independently to YouTube and Twitch. |
| Monitoring | Collect evidence about delivery, provider state, and measurement freshness. |
| Recovery source | Separate policy from the component that can change a process. |
| Public status publisher | Export filtered static information without exposing internal services. |

The detailed design has strict identifiers and ownership rules. Their practical
purpose is simple: a stale warning or uncertain command should not trigger
another action against the wrong process.

## What Was Measured

The same YouTube video identity was observed across a **160-day measurement
period**, ending at `2026-10-14 00:04:57 JST`. The operational ledger contains
**140 / 140 daily checkpoint dates** since the later ledger began, with one URL
hash and no recorded transitions. The earlier measurement start predates that
ledger. [The evidence note](28-day-same-url-sli-case-study.md) preserves both
counting periods and the older 14-day/28-day review.

This is evidence of URL preservation through operation and recovery. It is not
160 days of uninterrupted frames. The [scorecard](operational-scorecard.md)
separates that result from transport recovery, tests, and unmeasured risks.

## What The Failures Show

The [operational case studies](operational-case-studies.md) cover:

- a recurring network stall investigated with independent external connections;
- a watchdog whose old connection-port assumption disrupted a new cloud route;
- a Twitch renewal that initially recreated the YouTube forwarding handler;
- a warning that remained open after the current measurement source recovered;
- stale public-cache data and a broken external status-page string check; and
- a qualification harness that did not preserve enough evidence to establish a result.

Each story names the symptom, intervention, verification, and remaining limit.
Failures remain part of the record even when a later correction succeeds.

## Scope And Remaining Gaps

This is a single-operator system with a modest blast radius. It demonstrates
operational diagnosis, recovery boundaries, and honest measurement. It does not
claim commercial availability, multi-tenant scale, or disaster recovery from
power and disk loss.

The public CRA policy keeps production authorization disabled. The observed
OCI/Twitch operation is separate from native failure qualification, complete
crash/reconciliation testing, and a long reception soak. Those gates are not
presented as completed.

A reviewer can start with one [incident](operational-case-studies.md), then use
[the implementation map](implementation-review-map.md) to inspect its code and
regression tests. The full reference set is optional.
