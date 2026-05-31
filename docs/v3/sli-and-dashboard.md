# SLI And Dashboard Contract

The dashboard separates present state from historical degradation.

## Primary SLIs

- YouTube availability
- same URL preservation
- local ingest connected
- FFmpeg TCP send health
- audio route and RMS health
- now-playing freshness
- runtime memory guardrail
- recovery action safety

## Error Budget Rule

Availability and same-URL preservation are higher priority than visual quality
warnings. Encoder changes should not sacrifice delivery continuity unless the
operator explicitly accepts that tradeoff.

The upload ceiling is a warning boundary, not the tuning target. The current
5fps/3400k contract was accepted because the measured windows stayed below
5.0 Mbps while avoiding the larger per-frame quality loss of 10fps.

## Dashboard Caution

Long-window fields can be stale. Operators should compare dashboard signals
against fresh runtime evidence before deciding that the stream is currently
failing.
