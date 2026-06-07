# Compliance And Licensing Boundary

This is not legal advice. It records the pre-build engineering boundary used by the
operator when publishing a receive-only ADS-B visualization with NCS background
music. The purpose is to show how non-code constraints were turned into product
and operational design choices. Last reviewed: 2026-06-07.

## Scope

This document covers the public stream and the public repository snapshot:

- receive-only ADS-B ingest through Airspy, `airspy_adsb`, readsb, modified
  tar1090, and the `stream_v3` browser/FFmpeg delivery path;
- viewer-facing YouTube video, overlay panels, public-safe status snapshots,
  and public documentation;
- NCS music attribution for a YouTube livestream.

It does not certify that the setup is reusable in another jurisdiction, on
another platform, with another music catalog, or with a public raw ADS-B API. It
also does not claim authority to operate aircraft, ground transmitters, or ATC
services.

## ADS-B Radio Boundary

The source data is ADS-B traffic. The FAA describes ADS-B Out as a broadcast of
aircraft GPS location, altitude, ground speed, and related data to ground
stations and other aircraft about once per second:
<https://www.faa.gov/about/office_org/headquarters_offices/avs/offices/afx/afs/afs400/afs410/ads-b>.

For the Japanese radio-law risk surface, the relevant public text is the Radio
Act on e-Gov. Article 59 is framed around wireless communications directed to a
specific counterpart and prohibits disclosing or misusing intercepted existence
or content:
<https://laws.e-gov.go.jp/api/1/lawdata/325AC0000000131>.

The engineering posture is conservative:

- Treat ADS-B as a traffic-visibility broadcast, not as a private message feed
  addressed to this receiver.
- Keep the project receive-only. The public repository does not include a path
  for controlling or operating ADS-B transmitters.
- Do not turn raw RF reception into a public raw-data service. The published
  product is a video stream and reduced status evidence.
- Keep the viewer presentation oriented around traffic movement, aggregate
  receiver metrics, coverage, and operational health, not aircraft ownership or
  persistent tracking.

This posture does not depend on proving that every ADS-B element is legally
secret or non-secret. Instead, the design minimizes publication of sensitive
context even where the operator believes the underlying ADS-B broadcast is not a
one-to-one private communication.

## ADS-B Presentation Controls

The implementation keeps several privacy and product boundaries close to the
rendering path:

- `src/stream_core/overlay_server.py` removes receiver latitude and longitude
  from proxied `receiver.json`.
- The same proxy injects tar1090 configuration that hides the receiver site
  marker while preserving coverage/range guides.
- `ui/overlay/index.html` presents aggregate stream metrics such as target
  count, position count, message rate, coverage, receiver freshness, clocks, and
  now-playing state.
- The viewer frame includes a "Not for navigation" notice.
- Public status publication is reduced to a static snapshot and does not expose
  private Grafana, Prometheus, Loki, raw logs, credentials, home-network ingress,
  or retained runtime state.

Identifier handling is intentionally treated as a review boundary. The public
case-study claim does not require publishing ICAO addresses, registrations,
owner lookup results, or raw aircraft JSON. If the map renderer or public status
path is changed to surface persistent aircraft identifiers more prominently, the
change should be reviewed as a privacy and compliance change, not as a cosmetic
UI tweak.

## NCS Music Boundary

The music catalog is handled as a licensing constraint, not as a generic
"royalty-free" assumption. NCS states that independent creators may use NCS
music on YouTube or Twitch when the artist and song credits are placed in the
video or livestream description, and its FAQ also discusses monetized videos
under the same usage-policy condition:
<https://ncs.io/usage-policy/1/faqs-us...>.

The design rule is:

- The YouTube livestream description is the canonical attribution location.
- Credits should use the track-specific credit block from the NCS track page or
  the official NCS YouTube upload description when available.
- The overlay panel is supplemental viewer-facing disclosure. It is not treated
  as a replacement for description credits.
- Live chat or comment messages are not used as the compliance anchor because
  they are ephemeral, can scroll away, and are easy for replay viewers to miss.
- Local music files are excluded from the public repository snapshot.

This is why the public overlay can show "Music provided by NoCopyrightSounds"
while the durable licensing obligation remains outside the video frame in the
stream description.

## Re-Review Triggers

Re-review this boundary before any of the following changes:

- exposing the ADS-B JSON feed, readsb/tar1090 endpoint, receiver position, or
  raw aircraft logs as a public HTTP service;
- adding aircraft registration, owner lookup, persistent ICAO lists, or
  searchable aircraft history to the public presentation;
- changing from receive-only equipment to any transmitter or active RF
  operation;
- moving the stream to a platform whose music-credit or live-description model
  differs from YouTube;
- using non-NCS music, NCS tracks without track-specific attribution, sponsored
  brand content, or another commercial licensing model;
- publishing private operational evidence that was previously kept out of Git.

The useful reviewer signal is not that this file proves compliance. The signal
is that RF-derived data, public presentation, music licensing, source privacy,
and repository release scope are all treated as explicit design constraints.
