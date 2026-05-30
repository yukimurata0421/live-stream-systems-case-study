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

## Dashboard Caution

Long-window fields can be stale. Operators should compare dashboard signals
against fresh runtime evidence before deciding that the stream is currently
failing.
