# Map Mobile Legibility Review

Review date: 2026-08-11 JST

The prior frame was too dark for comfortable phone viewing. Aircraft colors
were visible, but the base geography, range references, and status text lost
too much contrast after the 1920x1080 broadcast frame was reduced by a mobile
player.

This document records a repository proposal. The adjusted renderer has not been
deployed to the live stream.

## Test Method And Result

A current 1920x1080 runtime frame was captured and reduced to two representative
player sizes. The proposed source was then rendered locally at the same base
resolution and reduced with the same method.

| Sample | Mean luma | Reading boundary |
| --- | ---: | --- |
| Current runtime frame | 43.48 | Aircraft colors remained visible; geography and card details were too dark. |
| Proposed day frame | 65.18 | About 50% higher mean luma; geography, precipitation, aircraft, and major card values remained distinct. |
| Proposed night frame | 51.23 | About 18% higher mean luma; night identity remained while coastlines and reference geometry stopped collapsing into black. |

At a 390x219 portrait inline size, no honest 1920x1080 dashboard design can
make all city labels and small status text readable. The acceptance target is
therefore aircraft, precipitation, and geographic context in portrait. At an
844x475 landscape/fullscreen size, the proposed frame makes the key values,
headings, aircraft, range rings, and coastline readable.

## Proposed Changes

- Raise locally calculated brightness from `0.00` to `0.07` at night and from
  `0.10` to `0.20` during day, with bounded twilight and golden-hour values.
- Increase terrain relief, coast, border, city, and airport contrast without
  recoloring operational aircraft or precipitation layers.
- Increase aircraft icon and position-dot scale.
- Strengthen coverage and range-ring width and opacity.
- Increase the primary overlay card width and the most important metric,
  title, and time text sizes.

The solar palette still changes only the base map. Aircraft state colors,
weather intensity, coverage geometry meaning, and alert semantics are
unchanged.

## Acceptance Boundary

Before any live rollout, a deployment-specific review should require:

1. a 1920x1080 capture from the proposed runtime;
2. 390x219 portrait and 844x475 landscape reductions;
3. confirmation that aircraft and precipitation colors remain semantically
   unchanged;
4. confirmation that map attribution remains visible;
5. a rollback threshold for excessive washout, label crowding, or loss of
   aircraft contrast; and
6. a post-rollout frame capture rather than an inference from source code.

Component tests assert the proposed numeric contract in
`tests/test_adsb_map_contract.py` and `tests/test_overlay_corner_contract.py`.
Passing those tests proves source consistency, not live mobile readability.

## Related Documents

- [`map-rendering-and-monitoring.md`](map-rendering-and-monitoring.md)
- [`map-production-cutover-case-study.md`](map-production-cutover-case-study.md)
- [`visual-audio-health-model.md`](visual-audio-health-model.md)
