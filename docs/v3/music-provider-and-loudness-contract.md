# Music Provider And Loudness Contract

Status: accepted
Last reviewed: 2026-09-11

## Purpose

The AutoDJ handles provider identity, credit, rotation, and relative loudness as
runtime contracts rather than cosmetic playlist details. This record publishes
the reviewable implementation boundary without publishing music files, private
permission correspondence, or production state.

## Provider And Schedule Boundary

The evening bucket is reserved for the reviewed Floracore collection from
`16:00` through `20:59` JST. NCS tracks are not mixed into that bucket. Other
time buckets retain their NCS rotation.

`ops/scripts/activate_floracore_evening.py` enforces three properties before it
changes a library:

- exactly 60 expected Floracore tracks are available;
- existing NCS evening links are archived without replacement; and
- an applying run requires the operator to confirm that public credit is in
  place.

The script defaults to a dry run. It refuses unexpected files, link targets,
and overwrite conditions rather than repairing them implicitly.

## Credit Boundary

Provider detection drives the viewer-facing badge and credit. Floracore
playback shows `FLORACORE · EVENING` and credits `@Floracore_EDM`; NCS playback
keeps the NoCopyrightSounds credit. The YouTube description remains the durable
credit location, so an overlay badge does not satisfy the activation gate by
itself.

NCS usage is governed by the NCS usage policy. The operator record describes a
separate permission for Floracore music in this YouTube Live stream, conditional
on credit. That permission is not generalized to redistribution, resale,
rights registration, another platform, or another channel.

## Permission-To-Operation Workflow

The Floracore addition was handled as a four-stage engineering and operational
workflow:

1. The operator asked the rights holder whether the music could be used in this
   YouTube Live stream. The resulting permission record remains operator-held,
   not published as repository evidence.
2. The permitted use and credit condition were made explicit. The scope was
   not generalized to redistribution, resale, rights registration, another
   platform, or another channel.
3. The credit requirement became an activation gate and a durable description
   requirement, with provider-specific overlay disclosure as a supplement.
4. Differences between providers became implementation and operations
   contracts: exclusive schedule buckets, provider detection, credit text,
   playback gain, source-file handling, and re-review triggers.

This is the point of recording the permission in an engineering case study:
the non-technical condition is traceable to code, tests, rollout gates, and an
ongoing operator obligation. The public repository proves those mechanisms,
not the private correspondence itself.

## Relative Loudness Boundary

The source MP3 files remain unmodified. Provider differences are compensated in
the decoded playback path:

| Setting | Value |
| --- | ---: |
| `AUTO_DJ_NCS_GAIN_DB` | `-6.7 dB` |
| `AUTO_DJ_FLORACORE_GAIN_DB` | `0.0 dB` |
| final `volume` filter | `0.540680` |

The NCS path therefore retains its earlier effective multiplier of approximately
`0.25`, while Floracore receives only the final-stage attenuation. This is a
fixed provider-relative correction, not dynamic normalization, compression, or
a claim that every track has identical perceived loudness.

## Public Evidence Boundary

The public snapshot proves that the following mechanisms and tests exist:

- provider-aware command construction and Unicode-safe now-playing text;
- exclusive evening-bucket planning with a credit-confirmation gate;
- provider-specific overlay labels; and
- source-file exclusion through the public release boundary.

It does not independently prove permission, the contents or hashes of private
music files, current YouTube description text, live activation, or audible
production output. Those require operator-held evidence and a current runtime
observation.

## Review Triggers

Re-review this contract before adding another provider, changing the time
window, altering provider detection, changing the gain model, re-encoding
source files, or moving playback to another platform or channel.

## Related Evidence

- `../../ops/scripts/activate_floracore_evening.py`
- `../../ops/scripts/prepare_floracore_music.py`
- `../../tests/test_activate_floracore_evening.py`
- `../../tests/test_auto_dj_rotation.py`
- `../../tests/test_overlay_corner_contract.py`
- `../compliance-and-licensing-boundary.md`
